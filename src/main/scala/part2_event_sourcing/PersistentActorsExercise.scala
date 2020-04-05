package part2_event_sourcing

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.FSM.Shutdown
import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import part2_event_sourcing.PersistentActor.InvoiceRecorded

import scala.collection.mutable

object PersistentActorsExercise extends App{
  /*
  persistent for voting
  Keep:
    - the citizens who voted
    - the poll: mapping between a candidate and the number of received votes so far
  The actor must be able to recover its state if it's shut down or restarted
   */

  //command, use this command as event too
  case class Vote(citizenPID: String, candidate: String)



  class VotingStation extends PersistentActor with ActorLogging{
    override def persistenceId: String = "simple-voting-station"
    //ignore the mutable state for now
    var citizens : mutable.Set[String] = new mutable.HashSet[String]()
    val poll: mutable.Map[String, Int] = new mutable.HashMap[String, Int]()

    override def receiveCommand: Receive = {
      case vote @ Vote(citizenPID, candidate) =>
        if(!citizens.contains(vote.citizenPID)) {
          //#1 event
          //#2 persist the event
          //#3 handle state change after persisting is successful
          //votedList.sea
          log.info(s"$citizenPID Voted  for candidate:$candidate")
          persist(vote) { _ => //COMMAND sourcing
            log.info(s"Persisted: $vote")
            handleInternalStateChange(vote.citizenPID, vote.candidate)
          }
        }
        else{
          log.warning(s"Citizen :$citizenPID try to vote more than one")
        }
      case "Print" =>
        log.info(s"Current state: \nCitizens $citizens \nCandidate $poll")
    }

    def handleInternalStateChange(citizenPID: String, candidate: String): Unit ={
        citizens.add(citizenPID)
        val votes = poll.getOrElse(candidate, 0)
        poll.put(candidate, votes + 1)
    }
    override def receiveRecover: Receive = {
      case vote @ Vote(citizenPID, candidate) =>
        log.info(s"Recovered : $vote")
        handleInternalStateChange(citizenPID, candidate)
    }
  }

  val system = ActorSystem("PersistenActorsExercises")
  val votingStation = system.actorOf(Props[VotingStation], "simpleVotingStation")

  val votesMap = Map[String, String] (
    "Alice" -> "Margin",
    "Bob" -> "Roland",
    "Charlie" -> "Martin",
    "David" -> "Jonas",
    "Daniel" -> "Martin",
  )

  //votesMap.keys.foreach{ citizen =>
  //  votingStation ! Vote(citizen, votesMap(citizen))
  //}
  votingStation ! Vote("Daniel", "Martin")
  votingStation ! "Print"
}
