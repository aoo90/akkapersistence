package part2_event_sourcing

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.FSM.Shutdown
import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

object PersistentActor extends App {

  /*
    scenario we have business and an accountant which keeps track of our invoices.
  */
  //command
  case class Invoice(recipient: String, date: Date, amount: Int)
  case class InvoiceBulk(invoice: List[Invoice])
  //EVENTS
  case class InvoiceRecorded(id: Int, recipient: String, date: Date, amount: Int)

  class Accountant extends PersistentActor with ActorLogging{
    var latestInvoiceId = 0
    var totalAmount = 0

    //should be unique per actor, very important
    override def persistenceId: String = "simple-accountant"

    //normal receive method
    override def receiveCommand: Receive = {
      case Invoice(recipient, date, amount) =>
        /*
        When you receive a command
        #1 you create an EVENT to persist into the store
        #2 you persist the event, the pass in a callback that will get triggered once the event
        is written
        #3 we update the actor's state when the event has persisted
         */
        log.info(s"Receive invoice for amount:$amount")
        val event = InvoiceRecorded(latestInvoiceId, recipient, date, amount)
        persist(event)/* during time gap, all messages are stashed*/ { e =>
          //async
          //safe to call back mutable state here, there is no race condition
          //update state
          latestInvoiceId += 1
          totalAmount += amount
          //correctly know the sender correctly
          sender() ! "PersistenceACK"
          log.info(s"Persisted $e as invoice #${e.id}, for total amount $totalAmount")
        }
      case InvoiceBulk(invoices) =>
        //#1 create events
        //#2 persist all events
        //#3 update the actor state when each event is persisted
        val invoiceIds = latestInvoiceId to (latestInvoiceId + invoices.size)
        val events = invoices.zip(invoiceIds).map{ pair =>
          val id = pair._2
          val invoice = pair._1

          InvoiceRecorded(id, invoice.recipient, invoice.date, invoice.amount)
        }

        persistAll(events){ e=>
          latestInvoiceId += 1
          totalAmount += e.amount
          log.info(s"Persisted SINGLE $e as invoice #${e.id} for total amount $totalAmount")
        }

        // can act like normal actor, no need to persist
      case Shutdown =>
        context.stop(self)
      case "print" =>
        log.info(s"Latest invoice id: $latestInvoiceId, totalamount: $totalAmount")
    }

    //handler call on recovery
    override def receiveRecover: Receive = {
      /*
      best practice: follow the logic in the persist steps of receiveCommand
       */
      case InvoiceRecorded(id, recipient, date, amount) =>
        latestInvoiceId = id
        totalAmount += amount
        log.info(s"Recovered invoice #$id, recipient: $recipient, date: $date, for amount " +
          s"$amount, " +
          s"total " +
          s"amount: $totalAmount")
    }
    //when persisting fails, this method will be called and the actor will be stopped, the data
    // some how mess up.
    //best practice: start the actor again after a while (use backoff supervisor)
    override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Fail to persist $event because of $cause")
      super.onPersistFailure(cause, event, seqNr)
    }

    //will be called when Journal throw exception
    //actor will be resume
    override def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Persist rejected for $event because of $cause")
      super.onPersistRejected(cause, event, seqNr)
    }
    //to persist, first run this for loop
    //to recover, run without this for loop
/*
    for(i <- 1 to 10){
      accountant ! Invoice(s"The Sofa Company $i", new SimpleDateFormat("dd/MM/yyyy").parse
      (s"01/01/202$i"), i *1000)
    }
    */

    //delete the journal target/rtjvm/journal
    val newInvoices = for(i <- 1 to 10) yield Invoice(s"The Sofa Company $i", new SimpleDateFormat
    ("dd/MM/yyyy").parse(s"01/01/202$i"), i *1000)
    //accountant ! InvoiceBulk(newInvoices.toList)


    /*
    Persistence failures

     */

    /*
    Persisting multiple events
    persistAll
     */
  }
  //src/main/resources/application.conf
  val system = ActorSystem("PersistentActors")
  val accountant = system.actorOf(Props[Accountant], "simpleAccountant")
  //NEVER EVER CALL PERSIST OR PERSISTALL FROM FUTURES
  //Shutdown of persistent actors
  //never call POISONPILL, actor will be shut down right away before persisting data
  //shutdown need to handle as message
}
