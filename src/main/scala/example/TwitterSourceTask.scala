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

//import example.TwitterSourceConnector;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util._;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import twitter4j.JSONObject;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class TwitterSourceTask extends SourceTask {

  private var queue: BlockingQueue[String] = null 
  private var client: BasicClient = null

  private var topic: String = null

  @Override
  def version(): String = new TwitterSourceConnector().version()

  @Override
  def start(props: Map[String, String]): Unit = {
    queue = new LinkedBlockingQueue(10000)
    topic = props.get(TwitterSourceConnector.TOPIC_CONFIG)

    val endpoint: StatusesFilterEndpoint = new StatusesFilterEndpoint()
    endpoint.stallWarnings(false)
    endpoint.trackTerms(Lists.newArrayList("money"))

    val auth: Authentication = new OAuth1(
      props.get(TwitterSourceConnector.CONSUMERKEY_CONFIG),
      props.get(TwitterSourceConnector.CONSUMERSECRET_CONFIG),
      props.get(TwitterSourceConnector.TOKEN_CONFIG),
      props.get(TwitterSourceConnector.SECRET_CONFIG))

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

    TwitterSourceTask.log.info("started")
  }

  @Override
  def poll(): List[SourceRecord] = {

    TwitterSourceTask.log.info("poll");
    if (client.isDone()) {
      System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
      null // TODO
    }

    val schema: Schema = SchemaBuilder.struct().name("tweet").field("text", Schema.STRING_SCHEMA).build();

    val msg: String = queue.poll(1, TimeUnit.SECONDS)

    if (msg == null) {
      TwitterSourceTask.log.info("Did not receive a message in 1 seconds")
      null
    } else {
      TwitterSourceTask.log.info(msg)
      try {
        val j: JSONObject = new JSONObject(msg)
        val records: List[SourceRecord] = new ArrayList[SourceRecord]()
        records.add(
          new SourceRecord(
            Collections.singletonMap("TODO", "TODO"),
            Collections.singletonMap("TODO2", "TODO2"),
            "twitter",
            schema,
            new Struct(schema).put("text",j.getString("text"))))
        records
      } catch {
        case _: Throwable => 
          TwitterSourceTask.log.info("BAD")
          null
      }
    }
  }

  @Override
  def stop(): Unit = {
    client.stop()
    TwitterSourceTask.log.info("stop")
  }

}

object TwitterSourceTask {
  //TODO: should be private
  val log: Logger = LoggerFactory.getLogger(classOf[TwitterSourceTask])
}