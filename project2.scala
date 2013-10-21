
import akka.actor.{Actor, Props, PoisonPill, Terminated}
import Actor._
import akka.actor.ActorSystem
import akka.actor.ActorRef
import scala.util.Random
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

object gossip {
  
  trait testGene
  case class converged(id: Int) extends testGene
  case class rumor(id: Int, message: Int) extends testGene
  case class pushSum(s: Double, w: Double) extends testGene
  case class create extends testGene
  case class terminate extends testGene
  case class removeNeighbor(removeId: Int) extends testGene
  case class result(value: Double) extends testGene
  
  
  object Topology extends Enumeration {
    type Topology = Value
    val Line, Full, Grid2D, imp2D = Value
  }
  
  object Algorithm extends Enumeration {
    type Algorithm = Value
    val Gossip, PushSum = Value
  }
  
  import Topology._
  import Algorithm._
  
  
  class Worker(id: Int, topology: Topology, algorithm: Algorithm, numActors: Int)  extends Actor {
    
      var msgCounter: Int = 0
      var termCounter = 0
      var message: Int = 0
      var isConverged: Boolean = false
      var hasMsg: Boolean = false
      val neighbors = new ArrayBuffer[Int]()
      var s: Double = id + 1
      var w: Double = 1
      
      set_topology()
      
      def set_topology() = {
        topology match {
          case Grid2D =>
            var dim: Int = Math.sqrt(numActors).toInt
            // check for top neighbor
            if (id - dim >= 0)  neighbors += id-dim
            // check for bottom neighbor
            if (id + dim < dim*dim) neighbors += id + dim
            // check for the left neighbor
            if (id%dim != 0)  neighbors += id-1
            // check for the right neighbor
            if (id%dim != dim-1)  neighbors += id+1
          case Line =>
            // check for left neighbor
            if( id-1 >= 0) neighbors+= id-1
            // check for right neighbor
            if(id+1 <= numActors-1) neighbors+= id+1
          case imp2D =>
             var dim: Int = Math.sqrt(numActors).toInt
            // check for top neighbor
            if (id - dim >= 0)  neighbors += id-dim
            // check for bottom neighbor
            if (id + dim < dim*dim) neighbors += id + dim
            // check for the left neighbor
            if (id%dim != 0)  neighbors += id-1
            // check for the right neighbor
            if (id%dim != dim-1)  neighbors += id+1
            // for one extra neighbor
            var t = Random.nextInt(numActors)
            while(t == id)
              t = Random.nextInt(numActors)
            neighbors+= t
        }
      }
      
      def getdestination(): Int = {
        var t:Int = 0
        topology match {
            case Line =>
              t = if (Math.random() > 0.5 && neighbors.length > 1) neighbors(1) else neighbors(0)
            case Full =>
              t = Random.nextInt(numActors)
              while(t == id)
                t = Random.nextInt(numActors)
            case Grid2D =>
              t = neighbors(Random.nextInt(neighbors.length))
            case imp2D =>
              t = neighbors(Random.nextInt(neighbors.length))
        }
        return t
      }

      def receive = {
      case rumor(source, message) =>
        if (isConverged == false) {
          msgCounter += 1
          this.message = message
          if (msgCounter <= GOSSIP_TERMINATION_COUNT - 1) {
            for (i <- 0 until numMessages)
              context.actorSelection("../" + getdestination().toString()) ! rumor(id, message)
          } else {
            context.parent ! converged(id)
            isConverged = true
          }
        }
      case pushSum(s, w) =>
        
          msgCounter += 1
          if (Math.abs((this.s/this.w) - ((this.s + s)/(this.w + w))) <= 1E-10)
            termCounter += 1
          else
            termCounter = 0
          
          if (termCounter == PUSH_SUM_TERM_COUNT - 1)
          {
              isConverged = true
              context.parent!result(this.s/this.w)
          }
          this.s += s
        this.w += w
          this.s /= 2
        this.w /= 2
        context.actorSelection("../" + getdestination().toString())!pushSum(this.s, this.w)

      case removeNeighbor(removeId) =>
        neighbors -= removeId
      }
    }
  
  class Master(topology: Topology, algorithm: Algorithm, numActors: Int) extends Actor {
    
    val convergedActors = new ListBuffer[Int]()
    val termFailed = new ListBuffer[Int]()
    val message: Int = 10
    var terminalCount: Int = numActors
    var startTime:Long = 0
    var convergedCount: Int = 0
    
    def receive = {
      case `create` =>
        // create the actors
        for(i <- 0 until numActors)
        context.actorOf(Props(new Worker(i, topology, algorithm, numActors)), i.toString);
        // start the algorithm
        startTime = System.currentTimeMillis
        algorithm match {
          case Gossip =>
            numMessages = 4
            context.actorSelection(Random.nextInt(numActors).toString())!rumor(0, message)
          case PushSum =>
            context.actorSelection(Random.nextInt(numActors).toString())!pushSum(0, 0)
        }
        
      case converged(id) =>
        //increase the converged actor count
        convergedCount+=1
        //Add the converged actor to the list
        convergedActors += id
        if (convergedCount == terminalCount)
        {
          var time_diff = System.currentTimeMillis() - startTime
          //println("System Terminating" + " count " + convergedCount)
          println("Convergence time is: " + time_diff.millis)
          exit
        }
        
       case result(value) =>
         var time_diff = System.currentTimeMillis() - startTime
         //println("The average is " + value)
         println("Convergence time is: " + time_diff.millis)
         exit
    }
  }
  
  def getNextPerfectSquare (num:Int):Int = {
      var nextNum: Double = math.sqrt(num)
      var temp = nextNum.toInt
      if (nextNum % 1 != 0) temp+=1
      (temp*temp)
  }

    var numMessages = 1
    val GOSSIP_TERMINATION_COUNT = 10
    val PUSH_SUM_TERM_COUNT = 3
    val failureN = 5
   
  def main(args: Array[String]) {
    if (args.length < 3)
    {
    print("Please run with 3 args\n");
    System.exit(1);
    }
      var numActors = args(0).toInt   
      val topology = {
        if (args(1).compareToIgnoreCase("full") == 0) Topology.Full
        else if (args(1).compareToIgnoreCase("2d") == 0) Topology.Grid2D
        else if (args(1).compareToIgnoreCase("line") == 0) Topology.Line
        else if (args(1).compareToIgnoreCase("imp2d") == 0) Topology.imp2D
        else
        {
          println("Please enter the correct topology")
          exit
        }
      }
      val algorithm = {
        if (args(2).compareToIgnoreCase("gossip") == 0) Algorithm.Gossip
        else if (args(2).compareToIgnoreCase("push-sum") == 0) Algorithm.PushSum
        else
        {
          println("Please enter the correct algorithm")
          exit
        }
      }
      
//      var numActors = 1000
//      val algorithm = Algorithm.PushSum
//      val topology = Topology.Grid2D
      println("Algorithm: " + algorithm + "; Topology: " + topology + "; No. of Nodes: " + numActors)
      if(topology == Grid2D || topology == imp2D)
      {
        numActors = getNextPerfectSquare(numActors)
      }
      val system = ActorSystem("Gossip-Pushsum")
      val master = system.actorOf(Props(new Master(topology, algorithm, numActors)), "master")
      master!create
    }
}