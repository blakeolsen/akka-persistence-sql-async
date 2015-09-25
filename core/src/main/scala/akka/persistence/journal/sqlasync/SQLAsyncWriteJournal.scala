package akka.persistence.journal.sqlasync

import akka.actor._
import akka.persistence.PersistentRepr

class MySQLAsyncWriteJournal extends ScalikeJDBCWriteJournal

class PostgreSQLAsyncWriteJournal extends ScalikeJDBCWriteJournal

private[persistence] object SQLAsyncWriteJournal {

  sealed trait SubscriptionCommand

  /**
   * Subscript the `sender` to changes for a specific `persistenceId`
   * Used by the query-side. The journal will send [[EventAppended]] messages to 
   * the subscriber when `asyncWriteMessages` has been called
   */
  final case class SubscribePersistenceId(persistenceId: String) extends SubscriptionCommand
  final case class EventAppended(persistenceId: String) extends DeadLetterSuppression

  /**
   * Subscribe the `sender` to current and new persistenceIds.
   * Used by query-side. The journal will send one [[CurrentPersistenceIds]] to the
   * subscriber followed by [[PersistenceIdAdded]] messages when new persistenceIds
   * are created.
   */
  final case object SubscribeAllPersistenceIds extends SubscriptionCommand
  final case class CurrentPersistenceIds(allPersistenceIds: Set[String]) extends DeadLetterSuppression
  final case class PersistenceIdAdded(persistenceId: String) extends DeadLetterSuppression

  /**
   * Subscribe the `sender` to changes (appended events) for a specific `tag`.
   * Used by query-side. The journal will send [[TaggedEventAppended]] messages to
   * the subscriber when `asyncWriteMessages` has been called.
   * Events are tagged by wrapping in [[akka.persistence.journal.Tagged]]
   * via an [[akka.persistence.journal.EventAdapter]].
   */
  final case class SubscribeTag(tag: String) extends SubscriptionCommand
  final case class TaggedEventAppended(tag: String) extends DeadLetterSuppression

  final case class ReplayTaggedMessages(fromSequenceNr: Long, toSequenceNr: Long, max: Long,
      tag: String, replyTo: ActorRef) extends SubscriptionCommand
  final case class ReplayedTaggedMessage(persistent: PersistentRepr, tag: String, offset: Long)
  extends DeadLetterSuppression with NoSerializationVerificationNeeded
}
