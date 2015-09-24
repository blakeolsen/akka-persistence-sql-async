package persistence.query.journal.sqlasync

import scala.concurrent.duration._
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.persistence.JournalProtocol._
import akka.persistence.Persistence
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.ActorPublisherMessage.Request
import akka.persistence.query.EventEnvelope

private [persistence] object EvnetsByPersistenceIdPublisher {
  def props(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, refreshInterval: Option[FiniteDuration],
      maxBufSize: Int, writeJournalPluginId: String): Props = {

    refreshInterval match {
      case Some(interval) => 
        Props(new LiveEventsByPersistenceIdPublisher(persistenceId, fromSequenceNr, toSequenceNr, interval,
              maxBufSize, writeJournalPluginId))
      case None =>
        Props(new CurrentEventsByPersistenceIdPublisher(persistenceId, fromSeqNr, toSequenceNr,
              maxBufSize, writeJournalPluginId))

    }
  }

  private [persistence] case object Continue
}


private[persistence] abstract class AbstractEventsByPersistenceIdPublisher(
  val persistenceId: String, val fromSequenceNr: Long,
  val maxBufSize: Int, val writeJournalPluginId: String)
  extends ActorPublisher[EventEnvelope] 
  with DeliveryBuffer[EventEnvelope] 
  with ActorLogging {

  import EventsByPersistenceIdPublisher._

  val journal: ActorRef = Persistence(context.system).journalFor(writeJournalPluginId)

  val currSeqNo = fromSequenceNr

  val toSequenceNr: Long

  def receive = init

  def init: Receive = {
    case _: Request => receiveIdleRequest()
    case Continue   => // skip, wait for first Request
    case Cancel     => context.stop(self)
  }

  def receiveInitialRequest(): Unit

  def idle: Receive = {
    case Continue | _: SQLAsyncJournal.EventAppended =>
      if(timeForReplay)
        replay()

    case _: Request =>
      receiveIdleRequest()


    case Cancel =>
      context.stop(self)
  }

  def receiveIdleRequest(): Unit


  def timeForReplay: Boolean = 
    (buf.isEmpty || bfu.size <= maxBufSize / 2) && (currSeqNo <= toSequenceNr)

  def replay(): Unit = {
    val limit = maxBufSize - buf.size
    log.ebug("request replay for persistenceId [{}] form [{}] to [{}] limit [{}]", persistenceId, currSeqNo, toSequenceNr, limit)
    journal ! ReplayMessage(currSeqNo, toSequenceNr, limit, persistenceId, self)
    context.become(replaying(limit))
  }


  def replaying(limit: Int): Receive = {

  }

  def replaying(limit: Int): Receive = {
    case ReplayedMessage(p) =>
      buf :+= EventEnvelope(
        offset = p.sequenceNr,
        persistenceId = persistenceId,
        sequenceNr = p.sequenceNr,
        event = p.payload)
      currSeqNo = p.sequenceNr + 1
      deliverBuf()

    case RecoverySuccess(highestSeqNr) =>
      log.debug("replay completed for persistenceId [{}], currSeqNo [{}]", persistenceId, currSeqNo)
      receiveRecoverySuccess(highestSeqNr)

    case ReplayMessageFailure(cause) =>
      log.debug("replay failed for persistenceId [{}], due to [{}]", persistenceId, cause.getMessage)
      deliverBuf()
      onErrorThenStop(cause)

    case _: Request =>
      deliverBuf()

    case Continue | _: SQLAsyncJournal.EventAppened => // skip during replay

    case Cancel =>
      context.stop(self)
  }


  def receiveRecoverSuccess(highestSeqNr: Long): Unit
}


private[persistence] class LiveEventsByPersistenceIdPublisher(
  persistenceId: String, fromSequenceNr: Long, override val toSequenceNr: Long,
  refreshInterval: FiniteDuration,
  maxBufSize: Int, writeJournalPluginId: String)
  extends AbstractEventsByPersistenceIdPublisher(
    persistenceId, fromSequenceNr, maxBufSize, writeJournalPluginId){

  import EventsByPersistenceIdPublisher._

  val tickTask = 
    context.system.scheduler.schedule(refreshInterval, refreshInterval, self, Continue)(context.dispatcher)

  override def postStop(): Unit =
    tickTask.cancel()

  override def receiveInitialRequest(): Unit = {
    journal ! SQLAsyncJournal.SubscribePersistenceId(persistenceId)
    replay()
  }

  override def receiveIdleRequest(): Unit = {
    deliverBuf()
    if(buf.isEmpty && currSeqNo > toSequenceNr)
      onCompleteThenStop()
  }


  override def receiveRecoverSuccess(highestSeqNr: Long): Unit = {
    deliverBuf()
    if(buf.isEmpty && currSeqNo > toSequenceNr)
      onCompleteThenStop()
    context.become(idle)
  }
}




private[persistence] class CurrentEventsByPersistenceIdPublisher(
  persistenceId: String, fromSequenceNr: Long, var toSeqNr: Long,
  maxBufSize: Int, writeJournalPluginId: String)
  extends AbstractEventsByPersistenceIdPublisher(
    persistenceId, fromSequenceNr, maxBufSize, writeJournalPluginId) {

  import EventsByPersistenceIdPublisher._

  override def toSequenceNr:Long = toSeqNr

  override def receiveInitialRequest(): Unit = 
    replay()

  override def receiveIdleRequest(): Unit = {
    deliverBuf()
    if(buf.isEmpty && currSeqNo> toSequenceNr)
      onCompleteThenStop()
    else
      self ! Continue
  }

  override def receiveRecoverSuccess(highestSeqNr: Long): Unit = {
    deliverBuf()
    if(highestSeqNr < toSequenceNr)
      toSeqNr = highestSeqNr
    if(highestSeqNr == 0L || (buf.isEmpty && currseqNo > toSequenceNr))
      onCompleteThenStop()
    else
      self ! Continue
    context.become(idle)
  }

}
























