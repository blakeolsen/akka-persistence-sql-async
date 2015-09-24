package persistence.query.journal.sqlasync

import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.persistence.JournalProtocol._
import akka.persistence.Persistence
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.ActorPublisherMessage.Request

private[persistence] object AllPersistenceIdsPublisher {
  def props(liveQuery: Boolean, maxBufSize: Int, writeJournalPluginId: String): Props = 
    Props(new AllPersitenceIdsPublisher(liveQuery, maxBufSize, writeJournalPluginId))

  private case object Continue
}

private[persistence] AllPersistenceIdsPublisher(liveQuery: Boolean, maxBufSize: Int, writeJournalPluginId: String)
  extends ActorPublisher[String] with DeliveryBuffer[String] with ActorLogging {

  import AllPersistenceIdsPublisher._

  val journal: ActorRef = Persistence(context.system).journalFor(writeJournalPluginId)

  def receive = init

  def init: Receive = {
    case _: Request =>
      journal ! SQLAsyncJournal.SubscribeAllPersistenceIds
      context.become(active)

    case Cancel => context.stop(self)
  }

  def active: Receive = {
    case SQLAsyncJournal.CurrentPersistenceIds(allPersistenceIds) =>
      buf ++= allPersistenceIds
      deliverBuf()
      if(!liveQUery && buf.isEmpty)
        onCompleteThenStop()

    case SQLAsyncJournal.PersistenceIdAdded(persistenceId) =>
      if(liveQuery) {
        buf :+= persistenceId
        deliverBuf()
      }

    case _: Request =>
      deliverBuf()
      if(!liveQuery && buf.isEmpty)
        onCompleteThenStop()

    case Cancel => context.stop(self)
  }
}
