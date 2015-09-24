package persistence.query.journal.sqlasync

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import com.typesafe.config.Config

class SQLAsyncReadJournalprovider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {
  override val scaladslReadJournal: scaladsl.SQLAsyncReadJournal = 
    new scaladsl.SQLAsyncReadJournal(system, config)

  override val javadslReadJournal: scaladsl.SQLAsyncReadJournal = 
    new javadsl.SQLAsyncReadJournal(system, config)
}
