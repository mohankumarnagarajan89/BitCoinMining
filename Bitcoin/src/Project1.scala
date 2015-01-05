import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import akka.routing.RoundRobinRouter
import scala.collection.mutable.HashMap
import java.security.MessageDigest
import scala.util.Random
import akka.actor.ActorSelection.toScala
import akka.actor.actorRef2Scala

object Project1 extends App
 {
 
// Check if the user is a Master or a Remote Worker

if(args(0).split("\\.").length ==4)
{
val workerRouter =  ActorSystem("WorkerSystem").actorOf(Props	[WorkerActor].withRouter(RoundRobinRouter(nrOfInstances = 			12)))
//Routes the message to all Workers
	for (i <- 1 until 13) 
	     workerRouter ! ServerIp(args(0))
}
else 
{
val masterActor = ActorSystem("ServerSystem").actorOf(Props		(new MasterActor(args(0).toInt)),name="MasterActor")
masterActor ! CreateWorker
}
}

class MasterActor(numberOfZeroes:Int) extends Actor  
 {
	var totaBitcoinsCount: Int = _
	var workersCompleted: Int = 1
	var start: Long = _
	var numberOfWorkers = 12
	var noOfInputs : Long = _
	var bitcoinsCount: Int = _ 
	var finalBitcoinMap = new HashMap[String,String]
	
	def receive = {
	  
  	case CreateWorker =>
	   	  val router =  context.actorOf(Props							[WorkerActor].withRouter(RoundRobinRouter					(nrOfInstances  = numberOfWorkers)))
	    	// Routes the message to all Workers
		    for (i <- 1 until numberOfWorkers+1)
		    {
		      router ! Work(i * 1000000, 1000000*(i+						1),numberOfZeroes,"Local")
		      start = System.currentTimeMillis()
		    }

  	case ScheduleWork(workerLocation) =>
  	  if(workerLocation.equalsIgnoreCase("Remote"))
  	  	  numberOfWorkers+=1
  	  sender ! Work(1, 1000000,numberOfZeroes,workerLocation)
	  	  	
  	case Result(bitcoinMap,workerLocation) =>
		    // handle result from the worker
	  	 bitcoinsCount =  bitcoinMap.size 
	  	 totaBitcoinsCount += bitcoinMap.size
	  	 noOfInputs += 1000000
		 println(workerLocation + " worker processed 						successfully ..")
	    	println(" \n\tNumber of Bitcoins found by this worker 					: "+bitcoinsCount)
		    		    
		bitcoinMap.foreach {keyVal => finalBitcoinMap.put					(keyVal._1,keyVal._2)}
		bitcoinMap.foreach {keyVal => println(keyVal._1 				+ "\t" + keyVal._2)}    
//Code to check if the server is running for more than 5 minutes
	  	  	
	  	if((System.currentTimeMillis() - start) < 300000)
	  	{
	  	  sender ! Work(1,								1000000,numberOfZeroes,workerLocation)
	  	}
	  	else
	  	{
	  	println("Number of workers completed : 							"+workersCompleted)
	    if (workersCompleted == numberOfWorkers)
	    {
	      finalBitcoinMap.foreach {keyVal => println(keyVal._1 				+ "\t" + keyVal._2)}
	      println("\nNumber of Inputs Processed :  							"+noOfInputs)
	      println("\nNumber of Bitcoins found : %s\nTotal time 				taken : %s millis".format							(totaBitcoinsCount, (System.currentTimeMillis - 					start)))
		 context.stop(self)
		}
		else
			workersCompleted += 1
	  	}
	}
 }


class WorkerActor extends Actor
{
  var noOfBitcoins = 0
  val sha = MessageDigest.getInstance("SHA-256")
  
  def receive = {
    
    case Work(start, nrOfElements, noOfZeroes,workerLocation) =>
      println(workerLocation + " worker started mining..")
      sender ! Result(calculateBitCoins(start, 				nrOfElements,noOfZeroes),workerLocation)
      
    case ServerIp(serverIp) =>
      println(serverIp)
      val serverActor = context.actorSelection("akka.tcp://ServerSystem@"+serverIp+":5187/user/MasterActor")
      serverActor ! ScheduleWork("Remote")
      
  }
  
  /**
   * Method to calculate the bitcoins based on the number of zeroes to be matched
   */
  def calculateBitCoins(start: Int, nrOfElements: Int, noOfZeroes:Int): HashMap[String,String] = {

    	var gatorId = "ashwinviswa"
	var hashValue = ""
	var inputString = ""
	var appendString = "abcde"
    	var bitcoinMap =  new HashMap[String,String]
    
	 for (i <- start until nrOfElements)
	  {
		inputString = gatorId + Random.nextLong()+appendString
		hashValue = sha.digest(inputString.getBytes).foldLeft("")((s: String, b: Byte) => s +Character.forDigit((b & 0xf0) >> 4, 16) + Character.forDigit(b & 0x0f, 16))
		
		if(hashValue.matches("0{"+noOfZeroes+"}[a-zA-Z0-9]*"))
		{
		  bitcoinMap.put(inputString,hashValue)
		}
	  }
    
    bitcoinMap 
 
}
  
}

/*
 * Case classes for Message transfer between server and workers
 */

sealed trait BitCoinMessage
 
case object CreateWorker extends BitCoinMessage
 
case class Work(start: Int, nrOfElements: Int, noOfZeroes: Int, workerLocation:String) extends BitCoinMessage
 
case class Result(value: HashMap[String,String], test:String) extends BitCoinMessage 

case class WorkerUpdate(bitcoinMap:HashMap[String,String]) extends BitCoinMessage 

case class ServerIp(value: String) extends BitCoinMessage 

case class ScheduleWork(workerLocation : String) extends BitCoinMessage
