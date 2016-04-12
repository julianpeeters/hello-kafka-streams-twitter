/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.examples;

import example.TwitterSourceConnector;
import org.apache.kafka.connect.api.ConnectEmbedded;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Embedded version of twitter-kafka-connect: https://github.com/rollulus/twitter-kafka-connect
 *
 * Kafka Connect Embedded implementation for connecting external data and publishing them into
 * kafka topic `twitter`. Creates maximum 1 task(s).
 *
 * NOTE: Cannot be converted to .scala because `BOOTSTRAP_SERVERS_CONFIG` and others seem to be
 * missing from `DistributedConfig` when translated to Scala. Why would this be???
 */

public class TwitterStreamDemo {

    private static final Logger log = LoggerFactory.getLogger(TwitterStreamDemo.class);

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws Exception {

        final String bootstrapServers = args.length == 1 ? args[0] : DEFAULT_BOOTSTRAP_SERVERS;

        // Launch Embedded Connect Instance for ingesting twitter feed filtered on "money" into twitter topic
        ConnectEmbedded connect = createTwitterSourceConnectInstance(bootstrapServers);
        connect.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                connect.stop();
            }
        });

        try {
            connect.awaitStop();
            log.info("Connect closed cleanly...");
        } finally {
            log.info("Everything closed cleanly...");
        }
    }

    private static ConnectEmbedded createTwitterSourceConnectInstance(String bootstrapServers) throws Exception {
        Properties workerProps = new Properties();
        workerProps.put(DistributedConfig.GROUP_ID_CONFIG, "twitter-connect");
        workerProps.put(DistributedConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "connect-configs");
        workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "connect-status");
        workerProps.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("key.converter.schemas.enable", "false");
        workerProps.put(DistributedConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter.schemas.enable", "false");
        workerProps.put(DistributedConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "30000");
        workerProps.put(DistributedConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put(DistributedConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter.schemas.enable", "false");

        Properties connectorProps = new Properties();
        connectorProps.put(ConnectorConfig.NAME_CONFIG, "twitter-source");
        connectorProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "example.TwitterSourceConnector");
        connectorProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        connectorProps.put(TwitterSourceConnector.CONSUMERKEY_CONFIG(), "*****");
        connectorProps.put(TwitterSourceConnector.CONSUMERSECRET_CONFIG(), "*****");
        connectorProps.put(TwitterSourceConnector.TOKEN_CONFIG(), "*****");
        connectorProps.put(TwitterSourceConnector.SECRET_CONFIG(), "*****");
        connectorProps.put(TwitterSourceConnector.TOPIC_CONFIG(), "twitter");

        return new ConnectEmbedded(workerProps, connectorProps);

    }

}
