package example
package reader

import models.Tweet

import org.apache.kafka.connect.source.SourceRecord;

import twitter4j.{ Status, TwitterObjectFactory }

import java.util.List

import scala.collection.JavaConverters._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object TwitterRecordBuilder {

  private val log: Logger = LoggerFactory.getLogger(classOf[TwitterSourceTask])

  def noMessages = {
    log.info("Did not receive a message in 1 seconds")
    null
  }

  def asRecord(
    topic: String,
    msg: String,
    records: List[SourceRecord]): List[SourceRecord] = try {
    if (msg == null) noMessages
    else {
      log.debug(msg)
      val status: Status = TwitterObjectFactory.createStatus(msg)
      val sourceRecord: SourceRecord = buildRecord(topic, status)
      // Reuse list so we don't have to make a new ArrayList for each message.
      records.clear
      records.add(sourceRecord)
      records
    }
  } catch {
    case _: Throwable => {
      log.info("BAD")
      null
    }
  }

  def buildRecord(topic: String, status: Status): SourceRecord = {
    val tweetStruct = Tweet.struct(Tweet(status))
    new SourceRecord(
      Map("tweetSource"-> status.getSource).asJava, //source partitions?
      Map("tweetId"-> status.getId).asJava, //source offsets?
      topic,
      tweetStruct.schema,
      tweetStruct)
  }

}