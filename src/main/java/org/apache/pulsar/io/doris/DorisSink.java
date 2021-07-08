/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.io.doris;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.*;
import org.apache.http.util.EntityUtils;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Connector(
        name = "doris",
        type = IOType.SINK,
        help = "A sink connector that sends pulsar messages to doris",
        configClass = DorisSinkConfig.class
)
@Slf4j
public class DorisSink implements Sink<GenericJsonRecord> {

    private String loadUrl = "";
    private DorisSinkConfig dorisSinkConfig;
    private CloseableHttpClient client;
    private HttpClientBuilder httpClientBuilder;
    private HttpPut httpPut;
    private int job_failure_retries = 2;
    private int job_label_repeat_retries = 3;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        dorisSinkConfig = DorisSinkConfig.load(config);

        // TODO:检查参数是否为null或者不合法
        String doris_host = dorisSinkConfig.getDoris_host();
        String doris_db = dorisSinkConfig.getDoris_db();
        String doris_table = dorisSinkConfig.getDoris_table();
        String doris_user = dorisSinkConfig.getDoris_user();
        String doris_password = dorisSinkConfig.getDoris_password();
        String doris_http_port = dorisSinkConfig.getDoris_http_port();
        job_failure_retries = Integer.parseInt(dorisSinkConfig.getJob_failure_retries());
        job_label_repeat_retries = Integer.parseInt(dorisSinkConfig.getJob_label_repeat_retries());
        // 是否应该ping一下这个ip？
        Objects.requireNonNull(doris_host, "Doris Host is not set");
        Objects.requireNonNull(doris_db, "Doris Database is not set");
        Objects.requireNonNull(doris_table, "Doris Table is not set");
        Objects.requireNonNull(doris_user, "Doris User is not set");
        Objects.requireNonNull(doris_password, "Doris Password is not set");
        // 是否应该查看port是否开启?
        Objects.requireNonNull(doris_http_port, "Doris HTTP Port is not set");
        // 是否应该mysql jdbc连接一下看能不能联通Doris?
        // 不知道运行时候报错能否显示详细exception，在此处完善校验处理?

        loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
                dorisSinkConfig.getDoris_host(),
                dorisSinkConfig.getDoris_http_port(),
                dorisSinkConfig.getDoris_db(),
                dorisSinkConfig.getDoris_table());

        /*DefaultServiceUnavailableRetryStrategy defaultServiceUnavailableRetryStrategy = new
                DefaultServiceUnavailableRetryStrategy();*/
        ServiceUnavailableRetryStrategy serviceUnavailableRetryStrategy = new MyServiceUnavailableRetryStrategy
                .Builder()
                .executionCount(3)
                .retryInterval(200)
                .build();

        httpClientBuilder = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                })
                .setServiceUnavailableRetryStrategy(serviceUnavailableRetryStrategy);

        client = httpClientBuilder.build();

        httpPut = new HttpPut(loadUrl);
        httpPut.setHeader(HttpHeaders.EXPECT, "100-continue");
        httpPut.setHeader(HttpHeaders.CONTENT_TYPE, "application/json;charset=UTF-8");
        httpPut.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(dorisSinkConfig.getDoris_user(),
                dorisSinkConfig.getDoris_password()));
        httpPut.setHeader("format", "json");
    }

    @Override
    public void write(Record<GenericJsonRecord> message) throws Exception {
        GenericJsonRecord record = message.getValue();
        JsonNode jsonNode = record.getJsonNode();
        ObjectMapper mapper = new ObjectMapper();
        String content = mapper.writeValueAsString(jsonNode);

        log.info("%%%%%%%%%%插入数据：  " + content);
        int failJobRetryCount = 0;
        int jobLabelRepeatRetryCount = 0;
        sendData(content, message, failJobRetryCount, jobLabelRepeatRetryCount);
    }

    private void sendData(String content,
                          Record<GenericJsonRecord> message,
                          int failJobRetryCount,
                          int jobLabelRepeatRetryCount) throws Exception {
        StringEntity entity = new StringEntity(content, "UTF-8");
        entity.setContentEncoding("UTF-8");

        String label = generateUniqueDorisLoadDataJobLabel();
        httpPut.setHeader("label", label);
        httpPut.setEntity(entity);

        CloseableHttpResponse response = client.execute(httpPut);
        String loadResult = "";
        if (response.getEntity() != null) {
            loadResult = EntityUtils.toString(response.getEntity());
        }

        log.info("@@@@@@@当前请求返回：" + loadResult);

        Map dorisLoadResultMap = parseDorisLoadResultJsonToMap(loadResult);
        processLoadJobResult(content, message, response, dorisLoadResultMap, failJobRetryCount, jobLabelRepeatRetryCount);
    }

    private void processLoadJobResult(String content, Record<GenericJsonRecord> message,
                                      CloseableHttpResponse response,
                                      Map dorisLoadResultMap,
                                      int failJobRetryCount,
                                      int jobLabelRepeatRetryCount) throws Exception {
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            message.fail();
            throw new IOException(String.format("Stream load failed, statusCode=%s", statusCode));
        }

        String jobStatus = dorisLoadResultMap.get("Status").toString();
        if (("Success".equals(jobStatus) || "Publish Timeout".equals(jobStatus))
                && "OK".equals(dorisLoadResultMap.get("Message"))) {
            message.ack();
            log.info("Job is success!");
        } else if ("Label Already Exists".equals(jobStatus)) {
            if (jobLabelRepeatRetryCount < job_label_repeat_retries) {
                String existingJobStatus = dorisLoadResultMap.get("ExistingJobStatus").toString();
                log.error("Doris label already exists! The existing job jobStatus is ： " + existingJobStatus);
                sendData(content, message, failJobRetryCount, jobLabelRepeatRetryCount + 1);
            } else {
                message.fail();
                log.error("Maximum number of retries exceeded(Job label repeat), message: " + content);
            }
        } else if ("Fail".equals(jobStatus)) {
            if (failJobRetryCount < job_failure_retries) {
                log.error("Job is fail,please retry! Error message: " + dorisLoadResultMap.get("Message").toString());
                sendData(content, message, failJobRetryCount + 1, jobLabelRepeatRetryCount);
            } else {
                message.fail();
                log.error("Maximum number of retries exceeded(Job fail), message: " + content);
                throw new Exception(String.format("Maximum number of retries exceeded(Job fail)!"));
            }
        } else {
            message.fail();
            log.error("Unknown error!");
        }
    }

    private static boolean isHostConnectable(String host, int port) {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port));
        } catch (IOException e) {
            log.error("Doris Fe is not available");
            e.printStackTrace();
            return false;
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    private Map parseDorisLoadResultJsonToMap(String loadResult) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Map dorisLoadResult = mapper.readValue(loadResult, Map.class);
        return dorisLoadResult;
    }

    private static String generateUniqueDorisLoadDataJobLabel() {
        Calendar calendar = Calendar.getInstance();
        String label = String.format("pulsar_io_%s%02d%02d_%02d%02d%02d_%s",
                calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND),
                UUID.randomUUID().toString().replaceAll("-", ""));
        return label;
    }

    private static String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded, StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
    }
}
