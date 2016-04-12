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

package example;

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

class TwitterSourceConnector extends SourceConnector {
    
    private var consumerkey: String = ""
    private var consumersecret: String = ""
    private var token: String = ""
    private var secret: String = ""

    @Override
    def version(): String = AppInfoParser.getVersion

    @Override
    def start(props: Map[String, String]): Unit = {
        TwitterSourceConnector.log.info("start")
        consumerkey = props.get(TwitterSourceConnector.CONSUMERKEY_CONFIG)
        consumersecret = props.get(TwitterSourceConnector.CONSUMERSECRET_CONFIG)
        token = props.get(TwitterSourceConnector.TOKEN_CONFIG)
        secret = props.get(TwitterSourceConnector.SECRET_CONFIG)
    }

    @Override
    def taskClass(): Class[_ <: Task] = classOf[TwitterSourceTask]

    @Override
    def taskConfigs(maxTasks: Int): List[Map[String, String]] = {
        TwitterSourceConnector.log.info("cfgs")
        val configs: ArrayList[Map[String, String]] = new ArrayList[Map[String, String]]()
        val config: Map[String, String] = new HashMap[String, String]()
        config.put(TwitterSourceConnector.CONSUMERKEY_CONFIG, consumerkey)
        config.put(TwitterSourceConnector.CONSUMERSECRET_CONFIG, consumersecret)
        config.put(TwitterSourceConnector.TOKEN_CONFIG, token)
        config.put(TwitterSourceConnector.SECRET_CONFIG, secret)
        configs.add(config)
        configs
    }

    @Override
    def stop(): Unit = {
    }

    @Override
    def config(): ConfigDef = {
      new ConfigDef()
        .define(TwitterSourceConnector.CONSUMERKEY_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Twitter API consumer key")
        .define(TwitterSourceConnector.CONSUMERSECRET_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Twitter API Consumer Secret")
        .define(TwitterSourceConnector.TOKEN_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Twitter API token")
        .define(TwitterSourceConnector.SECRET_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Twitter API secret")
        .define(TwitterSourceConnector.TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "kafka topic to publish the feed into");
    }

}

object TwitterSourceConnector {

  val TOPIC_CONFIG = "topic"
  val CONSUMERKEY_CONFIG = "*****"
  val CONSUMERSECRET_CONFIG = "*****"
  val TOKEN_CONFIG = "*****"
  val SECRET_CONFIG = "*****"

  // TODO: should be private
  val log: Logger = LoggerFactory.getLogger(classOf[TwitterSourceTask])
}