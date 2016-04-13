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
import reader.TwitterRecordBuilder

import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.ArrayList
import java.util.List
import java.util.Map

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.httpclient.auth.Authentication
import com.twitter.hbc.httpclient.auth.OAuth1
import twitter4j.Status

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class TwitterSourceTask extends SourceTask {

  private val log: Logger = LoggerFactory.getLogger(classOf[TwitterSourceTask])

  private var client: BasicClient = null
  private var queue: BlockingQueue[String] = null 
  private var topic: String = null
  private val records: List[SourceRecord] = new ArrayList[SourceRecord]()

  @Override
  def version(): String = new TwitterSourceConnector().version()

  @Override
  def start(props: Map[String, String]): Unit = {
    log.info("starting")

    queue = new LinkedBlockingQueue[String](10000)
    topic = props.get(TwitterSourceConfig.TOPIC_CONFIG)

    val endpoint: StatusesFilterEndpoint = new StatusesFilterEndpoint()
    endpoint.stallWarnings(false)
    endpoint.trackTerms(Lists.newArrayList("money"))

    val auth: Authentication = new OAuth1(
      props.get(TwitterSourceConfig.CONSUMER_KEY_CONFIG),
      props.get(TwitterSourceConfig.CONSUMER_SECRET_CONFIG),
      props.get(TwitterSourceConfig.TOKEN_CONFIG),
      props.get(TwitterSourceConfig.SECRET_CONFIG))

    // Create a new BasicClient. By default gzip is enabled.
    client = new ClientBuilder()
      .name("twitter-streaming-example")
      .hosts(Constants.STREAM_HOST)
      .endpoint(endpoint)
      .authentication(auth)
      .processor(new StringDelimitedProcessor(queue))
      .build()

    // Establish a connection
    client.connect()
  }

  @Override
  def poll(): List[SourceRecord] = {
    if (client.isDone()) {
      val exitMessage = client.getExitEvent().getMessage()
      System.out.println("Client connection closed unexpectedly: " + exitMessage);
      null // TODO
    }
    val msg: String = queue.poll(1, TimeUnit.SECONDS)
    TwitterRecordBuilder.asRecord(topic, msg, records)
  }

  @Override
  def stop(): Unit = {
    client.stop()
    log.info("stopping")
  }

}