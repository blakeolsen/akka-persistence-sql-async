package persistence.query.journal.sqlasync

import akka.stream.actor.ActorPublisher


private[persistence] trait DeliveryBuffer[T]{ _: ActorPublisher[T] =>

  var buf = Vector.empty[T]

  def deliverBuf(): Unit = 
    if(buf.nonEmpty && totalDemand > 0) {
      if(buf.size == 1){
        onNext(buf.head)
        buf = Vector.empty
      } else if (totalDemand <= Int.MaxValue) {
        val (use, keep) = buf.splitAt(totalDemand.toInt)
        buf = keep
        use foreach onNext
      } else {
        buf foreach onNext
        buf = Vector.empty
      }
    }

}
