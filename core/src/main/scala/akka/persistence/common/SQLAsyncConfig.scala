package akka.persistence.common

import akka.actor.ActorSystem

private[persistence] class SQLAsyncConfig(val system: ActorSystem) {
  val rootKey = "akka-persistence-sql-async"
  val config = system.settings.config.getConfig(rootKey)

  val user = config.getString("user")
  val password = config.getString("password")
  val url = config.getString("url")
  val maxPoolSize = config.getInt("max-pool-size")
  val waitQueueCapacity = config.getInt("wait-queue-capacity")
  val journalTableName = config.getString("journal-table-name")
  val snapshotTableName = config.getString("snapshot-table-name")
  val tagsTableName = config.getString("tags-table-name")
  val tagsBridgeTableName = config.getString("tags-bridge-table-name")
}

private[persistence] object SQLAsyncConfig {
  def apply(system: ActorSystem): SQLAsyncConfig = new SQLAsyncConfig(system)
}
