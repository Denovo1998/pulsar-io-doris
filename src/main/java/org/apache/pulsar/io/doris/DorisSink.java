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
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.*;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
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

    /**
     * Open connector with configuration
     *
     * @param config      initialization config
     * @param sinkContext
     * @throws Exception IO type exceptions when opening a connector
     */
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
        // 是否应该ping一下这个ip？
        Objects.requireNonNull(doris_host, "Doris Host is not set");
        Objects.requireNonNull(doris_db, "Doris Database is not set");
        Objects.requireNonNull(doris_table, "Doris Table is not set");
        Objects.requireNonNull(doris_user, "Doris User is not set");
        Objects.requireNonNull(doris_password, "Doris Password is not set");
        // 是否应该查看port是否开启
        Objects.requireNonNull(doris_http_port, "Doris HTTP Port is not set");
        // 是否应该mysql jdbc连接一下看能不能联通Doris
        // 不知道运行时候报错能否显示详细exception，在此处完善校验处理

        loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
                dorisSinkConfig.getDoris_host(),
                dorisSinkConfig.getDoris_http_port(),
                dorisSinkConfig.getDoris_db(),
                dorisSinkConfig.getDoris_table());

        DefaultServiceUnavailableRetryStrategy defaultServiceUnavailableRetryStrategy = new
                DefaultServiceUnavailableRetryStrategy();
        ServiceUnavailableRetryStrategy serviceUnavailableRetryStrategy = new MyServiceUnavailableRetryStrategy
                .Builder()
                .executionCount(3)
                .retryInterval(1000)
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

    /**
     * Write a message to Sink
     *
     * @param message message to write to sink
     * @throws Exception
     */
    @Override
    public void write(Record<GenericJsonRecord> message) throws Exception {
        GenericJsonRecord record = message.getValue();
        JsonNode jsonNode = record.getJsonNode();
        ObjectMapper mapper = new ObjectMapper();
        String content = mapper.writeValueAsString(jsonNode);

        log.info("%%%%%%%%%%插入数据：  " + content);
        sendData(content, dorisSinkConfig, message);
    }

    private void sendData(String content, DorisSinkConfig dorisSinkConfig,
                          Record<GenericJsonRecord> message) throws IOException {
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

        Map dorisLoadResultMap = parseDorisLoadResultJsonToPOJO(loadResult);
        processLoadJobResult(message, response, dorisLoadResultMap);
    }

    private void processLoadJobResult(Record<GenericJsonRecord> message,
                                      CloseableHttpResponse response,
                                      Map dorisLoadResultMap) throws IOException {
        final int statusCode = response.getStatusLine().getStatusCode();
        // 200只判定Doris服务正不正常
        if (statusCode != 200) {
            message.fail();
            // TODO: 这里表示指定节点服务不正常。添加参数所有fe的host和http_port(逗号分隔)，换个Fe URL重新提交？
            // TODO: 幂等：（1）使用之前的label：Status=Label Already Exists && ExistingJobStatus = FINISHED
            // TODO: （2）重新申请一个label：数据有可能重复？doris表解决，这里不管？
            throw new IOException(String.format("Stream load failed, statusCode=%s", statusCode));
        }
        /*{
            "TxnId": 35036,
                "Label": "pulsar_io_20210706_153835_668b99df21984a06a98493b6e96d3002",
                "Status": "Success",
                "Message": "OK",
                "NumberTotalRows": 1,
                "NumberLoadedRows": 1,
                "NumberFilteredRows": 0,
                "NumberUnselectedRows": 0,
                "LoadBytes": 37,
                "LoadTimeMs": 29,
                "BeginTxnTimeMs": 1,
                "StreamLoadPutTimeMs": 2,
                "ReadDataTimeMs": 0,
                "WriteDataTimeMs": 7,
                "CommitAndPublishTimeMs": 18
        }*/
        if ("Success".equals(dorisLoadResultMap.get("Status")) && "OK".equals(dorisLoadResultMap.get("Message"))) {
            message.ack();
        } else {
            message.fail();
        }
    }

    public static boolean isHostConnectable(String host, int port) {
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

    public Map parseDorisLoadResultJsonToPOJO(String loadResult) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Map dorisLoadResult = mapper.readValue(loadResult, Map.class);
        return dorisLoadResult;
    }

    public static String generateUniqueDorisLoadDataJobLabel() {
        Calendar calendar = Calendar.getInstance();
        String label = String.format("pulsar_io_%s%02d%02d_%02d%02d%02d_%s",
                calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND),
                UUID.randomUUID().toString().replaceAll("-", ""));
        return label;
    }

    public static String basicAuthHeader(String username, String password) {
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
