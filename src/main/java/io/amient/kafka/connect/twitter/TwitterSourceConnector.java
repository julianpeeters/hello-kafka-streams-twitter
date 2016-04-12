/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.kafka.connect.twitter;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TwitterSourceConnector extends SourceConnector {
    public static final String TOPIC_CONFIG = "topic";
    public static final String CONSUMERKEY_CONFIG = "*****";
    public static final String CONSUMERSECRET_CONFIG = "*****";
    public static final String TOKEN_CONFIG = "*****";
    public static final String SECRET_CONFIG = "*****";

    private static final Logger log = LoggerFactory.getLogger(TwitterSourceTask.class);

    private String consumerkey;
    private String consumersecret;
    private String token;
    private String secret;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("start");
        consumerkey = props.get(CONSUMERKEY_CONFIG);
        consumersecret = props.get(CONSUMERSECRET_CONFIG);
        token = props.get(TOKEN_CONFIG);
        secret = props.get(SECRET_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TwitterSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("cfgs");
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        config.put(CONSUMERKEY_CONFIG, consumerkey);
        config.put(CONSUMERSECRET_CONFIG, consumersecret);
        config.put(TOKEN_CONFIG, token);
        config.put(SECRET_CONFIG, secret);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(CONSUMERKEY_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Twitter API consumer key")
                .define(CONSUMERSECRET_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Twitter API Consumer Secret")
                .define(TOKEN_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Twitter API token")
                .define(SECRET_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Twitter API secret")
                .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "kafka topic to publish the feed into");
    }

}
