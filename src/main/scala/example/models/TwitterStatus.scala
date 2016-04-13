package example
package models

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import twitter4j.{Status, User}

/**
  * Adapted from andrew@datamountaineer.com 29/02/16. 
  * kafka-connect-twitter
  * 
  */
case class Tweet(id: Long, createdAt: String, favoriteCount: Int, text: String, user: TwitterUser)
case class TwitterUser(id: Long, name: String, screenName: String, location: String)

object TwitterUser {
  def apply(u: User) = {
    new TwitterUser(id = u.getId, name = u.getName, screenName = u.getScreenName, location = u.getLocation)
  }

  def struct(u: TwitterUser) =
    new Struct(schema)
      .put("id", u.id)
      .put("name", u.name)
      .put("screenName", u.screenName)
      .put("location", u.location)

  val schema = SchemaBuilder.struct().name("TwitterUser")
    .field("id", Schema.INT64_SCHEMA)
    .field("name", Schema.STRING_SCHEMA)
    .field("screenName", Schema.STRING_SCHEMA)
    .field("location", Schema.STRING_SCHEMA)
    .build()
}

object Tweet {
  def apply(s: Status) = {
    new Tweet(id = s.getId, createdAt = s.getCreatedAt.toString, favoriteCount = s.getFavoriteCount, text = s.getText, user = TwitterUser(s.getUser))
  }

  def struct(t: Tweet) =
    new Struct(schema)
      .put("id", t.id)
      .put("createdAt", t.createdAt)
      .put("favoriteCount", t.favoriteCount)
      .put("text", t.text)
      .put("user", TwitterUser.struct(t.user))

  val schema = SchemaBuilder.struct().name("Tweet")
    .field("id", Schema.INT64_SCHEMA)
    .field("createdAt", Schema.STRING_SCHEMA)
    .field("favoriteCount", Schema.INT32_SCHEMA)
    .field("text", Schema.STRING_SCHEMA)
    .field("user", TwitterUser.schema)
    .build()
}