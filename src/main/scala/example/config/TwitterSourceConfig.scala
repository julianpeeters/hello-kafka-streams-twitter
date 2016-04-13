package example
package config

import java.util

import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.config.ConfigDef.{Type, Importance}

/**
  * Created by andrew@datamountaineer.com on 24/02/16. 
  * kafka-connect-twitter
  */
object TwitterSourceConfig {
  val TOPIC_CONFIG = "topic"
  val CONSUMER_KEY_CONFIG = "twitter.consumerkey"
  val CONSUMER_KEY_CONFIG_DOC = "Twitter account consumer key."
  val CONSUMER_SECRET_CONFIG = "twitter.consumersecret"
  val CONSUMER_SECRET_CONFIG_DOC = "Twitter account consumer secret."
  val TOKEN_CONFIG = "twitter.token"
  val TOKEN_CONFIG_DOC = "Twitter account token."
  val SECRET_CONFIG = "twitter.secret"
  val SECRET_CONFIG_DOC = "Twitter account secret."
  val TRACK_TERMS = "track.terms"
  val TRACK_TERMS_DOC = "Twitter terms to track."
  val TRACK_TERMS_DEFAULT = ""
  val TWITTER_APP_NAME = "twitter.app.name"
  val TWITTER_APP_NAME_DOC = "Twitter app name"
  val TWITTER_APP_NAME_DEFAULT = "KafkaConnectTwitterSource"

  val config: ConfigDef = new ConfigDef()
        .define(CONSUMER_KEY_CONFIG, Type.STRING, Importance.HIGH, CONSUMER_KEY_CONFIG_DOC)
        .define(CONSUMER_SECRET_CONFIG, Type.STRING, Importance.HIGH, CONSUMER_SECRET_CONFIG_DOC)
        .define(TOKEN_CONFIG, Type.STRING, Importance.HIGH, TOKEN_CONFIG_DOC)
        .define(SECRET_CONFIG, Type.STRING, Importance.HIGH, SECRET_CONFIG_DOC)
        .define(TRACK_TERMS, Type.LIST, TRACK_TERMS_DEFAULT, Importance.LOW, SECRET_CONFIG_DOC)
        .define(TWITTER_APP_NAME, Type.LIST, TWITTER_APP_NAME_DEFAULT, Importance.HIGH, TWITTER_APP_NAME_DOC)
}

class TwitterSourceConfig(props: util.Map[String, String])
  extends AbstractConfig(TwitterSourceConfig.config, props) {
}

