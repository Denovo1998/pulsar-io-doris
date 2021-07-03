/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.io.doris;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * The base abstract class for ElasticSearch sinks.
 * Users need to implement extractKeyValue function to use this sink.
 * This class assumes that the input will be JSON documents
 */
@Connector(
        name = "doris",
        type = IOType.SINK,
        help = "A sink connector that sends pulsar messages to doris",
        configClass = DorisSinkConfig.class
)
@Slf4j
public class DorisSink implements Sink<byte[]> {

    private String loadUrl = "";
    private Properties properties = new Properties();
    private DorisSinkConfig dorisSinkConfig;
    private CloseableHttpClient client;
    private CloseableHttpResponse response;

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
    }

    /**
     * Write a message to Sink
     *
     * @param record record to write to sink
     * @throws Exception
     */
    @Override
    public void write(Record<byte[]> record) throws Exception {
        KeyValue<String, byte[]> keyValue = extractKeyValue(record);
        sendData(keyValue.getValue().toString(), dorisSinkConfig);
    }

    private void sendData(String content, DorisSinkConfig dorisSinkConfig) throws IOException {
        final String loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
                dorisSinkConfig.getDoris_host(),
                dorisSinkConfig.getDoris_http_port(),
                dorisSinkConfig.getDoris_db(),
                dorisSinkConfig.getDoris_table());

        final HttpClientBuilder httpClientBuilder = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });

        client = httpClientBuilder.build();

        HttpPut put = new HttpPut(loadUrl);
        StringEntity entity = new StringEntity(content, "UTF-8");
        put.setHeader(HttpHeaders.EXPECT, "100-continue");
        put.setHeader(HttpHeaders.CONTENT_TYPE, "text/plain; charset=UTF-8");
        put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(dorisSinkConfig.getDoris_user(),
                dorisSinkConfig.getDoris_password()));
        // the label header is optional, not necessary
        // use label header can ensure at most once semantics
        String label = generateLabel();
        put.setHeader("label", label);
        put.setEntity(entity);

        response = client.execute(put);
        String loadResult = "";
        if (response.getEntity() != null) {
            loadResult = EntityUtils.toString(response.getEntity());
        }
        final int statusCode = response.getStatusLine().getStatusCode();
        // statusCode 200 just indicates that doris be service is ok, not stream load
        // you should see the output content to find whether stream load is success
        if (statusCode != 200) {
            throw new IOException(
                    String.format("Stream load failed, statusCode=%s load result=%s", statusCode, loadResult));
        }
        log.info("@@@@@@@当前请求返回：" + loadResult);

        // 解析json
        Map dorisLoadResult = parseJsonToPOJO(loadResult);
        for (Object o : dorisLoadResult.keySet()) {
            log.info("##############key为" + o.toString());
            log.info("##############value为" + dorisLoadResult.get(o));
        }
    }

    public KeyValue<String, byte[]> extractKeyValue(Record<byte[]> record) {
        String key = record.getKey().orElse("");
        return new KeyValue<>(key, record.getValue());
    }

    /**
     * 构建请求头
     *
     * @param loadResult
     * @return
     */
    public Map parseJsonToPOJO(String loadResult) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Map dorisLoadResult = mapper.readValue(loadResult, Map.class);
        return dorisLoadResult;
    }

    /**
     * 生成doris导入任务的标识label
     *
     * @return
     */
    public static String generateLabel() {
        Calendar calendar = Calendar.getInstance();
        String label = String.format("pulsar_io_%s%02d%02d_%02d%02d%02d_%s",
                calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND),
                UUID.randomUUID().toString().replaceAll("-", ""));
        return label;
    }

    /**
     * 构建请求头
     *
     * @param username
     * @param password
     * @return
     */
    public static String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    @Override
    public void close() throws Exception {
    }
}
