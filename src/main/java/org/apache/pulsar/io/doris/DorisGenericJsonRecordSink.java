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
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.io.IOException;
import java.util.*;

@Connector(
        name = "doris",
        type = IOType.SINK,
        help = "A sink connector that sends pulsar messages to doris",
        configClass = DorisSinkConfig.class
)
@Slf4j
public class DorisGenericJsonRecordSink extends DorisAbstractSink<GenericJsonRecord> {

    public void sendData(List<Record<GenericJsonRecord>> swapRecordList,
                         int failJobRetryCount,
                         int jobLabelRepeatRetryCount) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode arrayNode = mapper.createArrayNode();
        for (Record<GenericJsonRecord> record : swapRecordList) {
            GenericJsonRecord message = record.getValue();
            JsonNode jsonNode = message.getJsonNode();
            arrayNode.add(jsonNode);
        }
        String content = mapper.writeValueAsString(arrayNode);
        // log.info("%%%插入数据：" + content);

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

        log.info("@@@@@@@当前请求返回json：" + loadResult);

        Map dorisLoadResultMap = parseDorisLoadResultJsonToMap(loadResult);
        processLoadJobResult(content, swapRecordList, response, dorisLoadResultMap, failJobRetryCount, jobLabelRepeatRetryCount);
    }

    @Override
    public void processLoadJobResult(String content,
                                     List<Record<GenericJsonRecord>> swapRecordList,
                                     CloseableHttpResponse response,
                                     Map dorisLoadResultMap,
                                     int failJobRetryCount,
                                     int jobLabelRepeatRetryCount) throws Exception {
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            swapRecordList.stream().forEach(Record::fail);
            throw new IOException(String.format("Stream load failed, statusCode=%s", statusCode));
        }

        String jobStatus = dorisLoadResultMap.get("Status").toString();
        if (("Success".equals(jobStatus) || "Publish Timeout".equals(jobStatus))
                && "OK".equals(dorisLoadResultMap.get("Message"))) {
            swapRecordList.stream().forEach(Record::ack);
            log.info("Job is success!");
        } else if ("Label Already Exists".equals(jobStatus)) {
            if (jobLabelRepeatRetryCount < job_label_repeat_retries) {
                String existingJobStatus = dorisLoadResultMap.get("ExistingJobStatus").toString();
                log.error("Doris label already exists! The existing job jobStatus is ： " + existingJobStatus);
                sendData(swapRecordList, failJobRetryCount, jobLabelRepeatRetryCount + 1);
            } else {
                swapRecordList.stream().forEach(Record::fail);
                log.error("Maximum number of retries exceeded(Job label repeat), message: " + content);
            }
        } else if ("Fail".equals(jobStatus)) {
            if (failJobRetryCount < job_failure_retries) {
                log.error("Job is fail,please retry! Error message: " + dorisLoadResultMap.get("Message").toString());
                sendData(swapRecordList, failJobRetryCount + 1, jobLabelRepeatRetryCount);
            } else {
                swapRecordList.stream().forEach(Record::fail);
                log.error("Maximum number of retries exceeded(Job fail), message: " + content);
                throw new Exception(String.format("Maximum number of retries exceeded(Job fail)!"));
            }
        } else {
            swapRecordList.stream().forEach(Record::fail);
            log.error("Unknown error!");
        }
    }

    private static String generateUniqueDorisLoadDataJobLabel() {
        Calendar calendar = Calendar.getInstance();
        String label = String.format("pulsar_io_%s%02d%02d_%02d%02d%02d_%s",
                calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND),
                UUID.randomUUID().toString().replaceAll("-", ""));
        return label;
    }

    private Map parseDorisLoadResultJsonToMap(String loadResult) {
        ObjectMapper mapper = new ObjectMapper();
        Map dorisLoadResult = null;
        try {
            dorisLoadResult = mapper.readValue(loadResult, Map.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return dorisLoadResult;
    }
}
