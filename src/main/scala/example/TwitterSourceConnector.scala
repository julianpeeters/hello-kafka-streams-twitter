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
package example

import config.TwitterSourceConfig

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.errors.ConnectException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.List
import java.util.Map

import scala.util.{Failure, Try}
import scala.collection.JavaConverters._

class TwitterSourceConnector extends SourceConnector {
  private val log: Logger = LoggerFactory.getLogger(classOf[TwitterSourceTask])
  private var configProps : Map[String, String] = null

  @Override
  def version(): String = AppInfoParser.getVersion

  @Override
  def start(props: Map[String, String]): Unit = {
    log.info(s"Starting Twitter source task with ${props.toString}.")
    configProps = props
    Try(new TwitterSourceConfig(props)) match {
      case Failure(f) => throw new ConnectException("Couldn't start Twitter source due to configuration error: "
        + f.getMessage, f)
      case _ =>
    }
  }

  @Override
  def taskClass(): Class[_ <: Task] = classOf[TwitterSourceTask]

  @Override
  def taskConfigs(maxTasks: Int): List[Map[String, String]] = {
    log.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(c => configProps).toList.asJava
  }

  @Override
  def stop(): Unit = {
  }

  @Override
  def config(): ConfigDef = {
    new ConfigDef()
    .define(TwitterSourceConfig.CONSUMER_KEY_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Twitter API consumer key")
    .define(TwitterSourceConfig.CONSUMER_SECRET_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Twitter API Consumer Secret")
    .define(TwitterSourceConfig.TOKEN_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Twitter API token")
    .define(TwitterSourceConfig.SECRET_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Twitter API secret")
    .define(TwitterSourceConfig.TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "kafka topic to publish the feed into");
  }

}
