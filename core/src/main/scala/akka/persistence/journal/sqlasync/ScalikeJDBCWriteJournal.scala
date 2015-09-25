package akka.persistence.journal.sqlasync

import akka.actor.{Actor, ActorLogging, ActorRef, Terminated}
import akka.persistence.common.{ScalikeJDBCExtension, ScalikeJDBCSessionProvider}
import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.persistence.JournalProtocol.{RecoverySuccess,ReplayMessagesFailure}
import akka.serialization.{Serialization, SerializationExtension}
import akka.pattern.pipe
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scalikejdbc._
import scalikejdbc.async._
import scala.collection.mutable
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

private[sqlasync] trait ScalikeJDBCWriteJournal extends AsyncWriteJournal with ActorLogging {
  import SQLAsyncWriteJournal._
  import context.dispatcher

  private[this] val serialization: Serialization = SerializationExtension(context.system)
  private[this] lazy val extension: ScalikeJDBCExtension = ScalikeJDBCExtension(context.system)
  private[this] lazy val sessionProvider: ScalikeJDBCSessionProvider = extension.sessionProvider
  private[this] lazy val table = {
    val tableName = extension.config.journalTableName
    SQLSyntaxSupportFeature.verifyTableName(tableName)
    SQLSyntax.createUnsafely(tableName)
  }

  private[this] lazy val tagsTable = {
    val tableName = extension.config.tagsTableName
    SQLSyntaxSupportFeature.verifyTableName(tableName)
    SQLSyntax.createUnsafely(tableName)
  }

  private[this] lazy val tagsBridgeTable = {
    val tableName = extension.config.tagsBridgeTableName
    SQLSyntaxSupportFeature.verifyTableName(tableName)
    SQLSyntax.createUnsafely(tableName)
  }

  private val persistenceIdSubscribers = new mutable.HashMap[String, mutable.Set[ActorRef]] with mutable.MultiMap[String, ActorRef]
  private val tagSubscribers = new mutable.HashMap[String, mutable.Set[ActorRef]] with mutable.MultiMap[String, ActorRef]
  private var allPersistenceIds = Set.empty[String]
  private var allPersistenceIdsSubscribers = Set.empty[ActorRef]

  private var tagSequenceNr = Map.empty[String, Long]
  private val tagPersistenceIdPrefix = "$$$"

  override def preStart = {
    loadAllPersistenceIds() 
      .map(allPersistenceIds = _)
      .onFailure{case e => throw e}
    super.preStart
  }


  override def receivePluginInternal: Receive = {
    case r @ ReplayTaggedMessages(fromSeqNr, toSeqNr, max, tag, replyTo) =>
      asyncReadHighestTagSeqNr(tag) flatMap { highSeqNr: Long =>
        val maxSeqNr = math.min(toSeqNr, highSeqNr)
        if(highSeqNr == 0L || fromSeqNr > maxSeqNr)
          Future.successful(highSeqNr)
        else {
          asyncReplayTaggedMessages(tag, fromSeqNr, maxSeqNr, max) {
            case ReplayedTaggedMessage(p, tag, offset) =>
              adaptFromJournal(p).foreach { adaptedPR: PersistentRepr =>
                replyTo.tell(ReplayedTaggedMessage(adaptedPR, tag, offset), Actor.noSender)
              }
          }.map(_ => highSeqNr)
        }
      } map {
        highSeqNr => RecoverySuccess(highSeqNr)
      } recover {
        case e => ReplayMessagesFailure(e)
      } pipeTo replyTo

    case SubscribePersistenceId(persistenceId: String) =>
      addPersistenceIdSubscriber(sender(), persistenceId)
        context.watch(sender())

    case SubscribeAllPersistenceIds =>
      addAllPersistenceIdsSubscriber(sender())
      context.watch(sender())

    case SubscribeTag(tag: String) =>
      addTagSubscriber(sender(), tag)
      context.watch(sender())

    case Terminated(ref) =>
      removeSubscriber(ref)
  }


  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {

    log.debug("Write {} messages: {}", messages.length, messages)
    var persistenceIds = Set.empty[String]
    var allTags = Set.empty[String]
    val batch = mutable.ListBuffer.empty[SQLSyntax]

    val results = messages.map { msg =>
      msg.payload.foldLeft[Try[immutable.List[SQLSyntax]]](Success(Nil)) { 
        case (Failure(e), _) => Failure(e)
        case (Success(ss: List[SQLSyntax]), pr: PersistentRepr) =>
          for {
            pr <- validatePersitenceId(pr)
            pr2 = pr.payload match {
                case Tagged(payload, tags) =>
                  if(tags.nonEmpty) allTags ++= tags
                  pr.withPayload(payload)
                case _ => pr
              }
            bytes <- serialization.serialize(pr2) 
          } yield {
            persistenceIds += pr.persistenceId
            ss :+ sqls"(${pr2.persistenceId}, ${pr2.sequenceNr}, $bytes)"
          }
      }.map{ xs: List[SQLSyntax] => batch.appendAll(xs) }
    }


    val records = sqls.csv(batch: _*)
    val sql = sql"INSERT INTO $table (persistence_id, sequence_nr, message) VALUES $records"
    log.debug("Execute {}, binding {}", sql.statement, messages)
    sessionProvider.localTx { implicit session =>
      sql.update().future() 
    } map { _ =>

      if(hasPersistenceIdSubscribers)
        persistenceIds foreach notifyPersistenceIdChange

      if(hasTagSubscribers && allTags.nonEmpty)
        allTags foreach notifyTagChange

      results
    }
  }


  def validatePersitenceId(pr: PersistentRepr): Try[PersistentRepr] = 
    if(pr.persistenceId.startsWith(tagPersistenceIdPrefix))
      Failure(new IllegalArgumentException(s"persistenceId [${pr.persistenceId}] must not start with $tagPersistenceIdPrefix"))
    else
      Success(pr)


  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.debug("Delete messages, persistenceId = {}, toSequenceNr = {}", persistenceId, toSequenceNr)
    sessionProvider.localTx { implicit session =>
      val sql = sql"DELETE FROM $table WHERE persistence_id = $persistenceId AND sequence_nr <= $toSequenceNr"
      log.debug("Execute {}, binding persistence_id = {} and sequence_nr = {}", sql.statement, persistenceId, toSequenceNr)
      sql.update().future().map(_ => ())
    }
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = {
    log.debug("Replay messages, persistenceId = {}, fromSequenceNr = {}, toSequenceNr = {}", persistenceId, fromSequenceNr, toSequenceNr)
    sessionProvider.withPool { implicit session =>
      val sql = sql"SELECT message FROM $table WHERE persistence_id = $persistenceId AND sequence_nr >= $fromSequenceNr AND sequence_nr <= $toSequenceNr ORDER BY sequence_nr ASC LIMIT $max"
      log.debug("Execute {}, binding persistence_id = {}, from_sequence_nr = {}, to_sequence_nr = {}", sql.statement, persistenceId, fromSequenceNr, toSequenceNr)
      sql.map(_.bytes("message"))
       .list()
       .future()
       .map(deserializeMessages)
       .map(_.foreach(replayCallback))
    }
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug("Read the highest sequence number, persistenceId = {}, fromSequenceNr = {}", persistenceId, fromSequenceNr)
    sessionProvider.withPool { implicit session =>
      val sql = sql"SELECT sequence_nr FROM $table WHERE persistence_id = $persistenceId ORDER BY sequence_nr DESC LIMIT 1"
      log.debug("Execute {} binding persistence_id = {} and sequence_nr = {}", sql.statement, persistenceId, fromSequenceNr)
      sql.map(_.longOpt(1)).single().future().map(_.flatten.getOrElse(0L))
    }
  }


  protected def loadAllPersistenceIds(): Future[Set[String]] = {
    sessionProvider.withPool { implicit session =>
      val sql = sql"SELECT distinct(persistence_id) FROM $table"
      log.debug("Execute {}", sql.statement)
      sql.map(_.string(1)).list.future().map( _.toSet )
    }
  }

  protected def asyncReadHighestTagSeqNr(tag: String): Future[Long] = {
    sessionProvider.withPool { implicit session =>
      val sql = sql"SELECT max(sequence_nr) FROM $table WHERE tag = $tag"
      log.debug("Execute {} binding tag = {}", sql.statement, tag)
      // This defaults to 0L if the specified tag isn't found as per
      // the logic found in:
      // `akka.persistence.journal.leveldb.LeveldbRecover.readHighestSequenceNr`
      sql.map(_.long(1)).single().future().map(_.getOrElse(0L))
    }
  }

  protected def asyncReplayTaggedMessages(tag: String, fromSeqNr: Long, toSeqNr: Long, limit: Long)(replayCallback: ReplayedTaggedMessage => Unit): Future[Unit] = {
    sessionProvider.withPool { implicit session =>
      val sqlStr = """
        |SELECT
        |  j.message,
        |  t.sequence_nr
        |FROM 
        |  $table j,
        |  $tagsTable t,
        |  $tagsBridgeTable b
        |WHERE 
        |      t.tag = $tag
        |  AND t.sequence_number >= $fromSeqNr
        |  AND t.sequence_number < $toSeqNr
        |  AND b.tag_id = t.tag_id
        |  AND b.persistence_id = j.persistence_id
        |ORDER BY
        |  t.sequence_nr ASC
        |LIMIT $limit
      """.stripMargin

      val sqlStmt = SQL(sqlStr)

      log.debug("Execute {} binding fromSeqNr = {}, toSeqNr = {}, limit = {}", 
          sqlStmt.statement, fromSeqNr, toSeqNr, limit)

      sqlStmt.map { r => (r.bytes(1), r.long(2)) }
       .list()
       .future()
       .map { msgSeqNrTpls: List[(Array[Byte], Long)] =>
         msgSeqNrTpls foreach { tpl =>
           val (msgBytes, seqNr) = tpl
           val pr = deserializeMessage(msgBytes)
           replayCallback(ReplayedTaggedMessage(pr, tag, seqNr))
         }
       }
    }
  }

  protected def deserializeMessages(msgBytesLst: List[Array[Byte]]) = 
    msgBytesLst.map(deserializeMessage)

  protected def deserializeMessage(msgBytes: Array[Byte]): PersistentRepr =
    serialization.deserialize(msgBytes, classOf[PersistentRepr]).get

  protected def hasPersistenceIdSubscribers: Boolean = persistenceIdSubscribers.nonEmpty

  protected def addPersistenceIdSubscriber(subscriber: ActorRef, persistenceId: String): Unit =
    persistenceIdSubscribers.addBinding(persistenceId, subscriber)

  protected def removeSubscriber(subscriber: ActorRef): Unit = {
    val keys = persistenceIdSubscribers.collect { case (k, s) if s.contains(subscriber) => k }
    keys.foreach { key => persistenceIdSubscribers.removeBinding(key, subscriber) }

    val tagKeys = tagSubscribers.collect { case (k, s) if s.contains(subscriber) => k }
    tagKeys.foreach { key => tagSubscribers.removeBinding(key, subscriber) }

    allPersistenceIdsSubscribers -= subscriber
  }

  protected def hasTagSubscribers: Boolean = tagSubscribers.nonEmpty

  protected def addTagSubscriber(subscriber: ActorRef, tag: String): Unit =
    tagSubscribers.addBinding(tag, subscriber)

  protected def hasAllPersistenceIdsSubscribers: Boolean = allPersistenceIdsSubscribers.nonEmpty

  protected def addAllPersistenceIdsSubscriber(subscriber: ActorRef): Unit = {
    allPersistenceIdsSubscribers += subscriber
    subscriber ! SQLAsyncWriteJournal.CurrentPersistenceIds(allPersistenceIds)
  }


  private def notifyPersistenceIdChange(persistenceId: String): Unit = 
    if(persistenceIdSubscribers.contains(persistenceId)) {
      val change = SQLAsyncWriteJournal.EventAppended(persistenceId)
      persistenceIdSubscribers(persistenceId).foreach(_ ! change)
    }

  private def notifyTagChange(tag: String): Unit =
    if(tagSubscribers.contains(tag)) {
      val changed = SQLAsyncWriteJournal.TaggedEventAppended(tag)
      tagSubscribers(tag).foreach(_ ! changed)
    }
}
