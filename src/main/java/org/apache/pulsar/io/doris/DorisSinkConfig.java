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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * @author liusinan
 * @version 1.0.0
 * @ClassName DorisSinkConfig.java
 * @Description doris config
 * curl --location-trusted -u root -H "label:123" -H "where: k1=20180601" -T testData
 * http://host:port/api/testDb/testTbl/_stream_load
 * @createTime 2021年07月02日 19:04:00
 */
@Data
@Accessors(chain = true)
public class DorisSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = true,
        defaultValue = "xxx.com",
        help =
            "A comma-separated list of host and port pairs that are the addresses of "
          + "the Kafka brokers that a Kafka client connects to initially bootstrap itself")
    private String doris_host;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help =
            "A comma-separated list of host and port pairs that are the addresses of "
          + "the Kafka brokers that a Kafka client connects to initially bootstrap itself")
    private String doris_db;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help =
            "A comma-separated list of host and port pairs that are the addresses of "
          + "the Kafka brokers that a Kafka client connects to initially bootstrap itself")
    private String doris_table;

    @FieldDoc(
        required = true,
        defaultValue = "root",
        help =
            "A comma-separated list of host and port pairs that are the addresses of "
          + "the Kafka brokers that a Kafka client connects to initially bootstrap itself")
    private String doris_user;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help =
            "A comma-separated list of host and port pairs that are the addresses of "
          + "the Kafka brokers that a Kafka client connects to initially bootstrap itself")
    private String doris_password;

    @FieldDoc(
        required = true,
        defaultValue = "8410",
        help =
            "A comma-separated list of host and port pairs that are the addresses of "
          + "the Kafka brokers that a Kafka client connects to initially bootstrap itself")
    private String doris_http_port;

    public static DorisSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), DorisSinkConfig.class);
    }

    public static DorisSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), DorisSinkConfig.class);
    }

}
