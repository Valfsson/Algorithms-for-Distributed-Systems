//Ballot Leader Election
import se.sics.kompics.network._
import se.sics.kompics.sl._
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}
import se.sics.kompics.{KompicsEvent, Start}
import scala.collection.mutable

//Provided Primitives to use in your implementation

  case class CheckTimeout(timeout: ScheduleTimeout) extends Timeout(timeout);

  case class HeartbeatReq(round: Long, highestBallot: Long) extends KompicsEvent;

  case class HeartbeatResp(round: Long, ballot: Long) extends KompicsEvent;

  private val ballotOne = 0x0100000000l;
  
  def ballotFromNAddress(n: Int, adr: Address): Long = {
    val nBytes = com.google.common.primitives.Ints.toByteArray(n);
    val addrBytes = com.google.common.primitives.Ints.toByteArray(adr.hashCode());
    val bytes = nBytes ++ addrBytes;
    val r = com.google.common.primitives.Longs.fromByteArray(bytes);
    assert(r > 0); // should not produce negative numbers!
    r
  }

  def incrementBallotBy(ballot: Long, inc: Int): Long = {
    ballot + inc.toLong * ballotOne
  }

  private def incrementBallot(ballot: Long): Long = {
    ballot + ballotOne
  }

class GossipLeaderElection(init: Init[GossipLeaderElection]) extends ComponentDefinition {

  val ble = provides[BallotLeaderElection];
  val pl = requires[PerfectLink];
  val timer = requires[Timer];

  val self = init match {
    case Init(s: Address) => s
  }
  val topology = cfg.getValue[List[Address]]("ble.simulation.topology");
  val delta = cfg.getValue[Long]("ble.simulation.delay");
  val majority = (topology.size / 2) + 1;

  private var period = cfg.getValue[Long]("ble.simulation.delay");
  private val ballots = mutable.Map.empty[Address, Long];

  private var round = 0l;
  private var ballot = ballotFromNAddress(0, self);

  private var leader: Option[(Long, Address)] = None;
  private var highestBallot: Long = ballot;

  private def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(period);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  private def makeLeader(topProcess: (Long, Address)) {
  }
  
  private def checkLeader() {
        ballots+=((self,ballot)) 
            println(s" list by long:  $ballots");
        val sorted=ballots.toSeq.sortBy(_._2); 
         println(s"sorted list by long:  $sorted");
        val top =sorted(0);
        var(topProcess, topBallot)=top;
            println(s"top  value:  $top");
        
        if(topBallot<highestBallot){
           
            while(ballot<=highestBallot){
             //  println(s"highestBalot:  $ballot vs balot:  $ballot");
              // println(s"balot:  $ballot");
               ballot+= 0x0100000000l;         //incrementBallot();
               // println(s" new balot:  $ballot");
            }
        leader = None;  
         println(s"leader none:  $leader");
        }else{
             println(s"leader:  $leader");
              println(s"top:  $top");
            if(Some(topBallot,topProcess)!=leader){
                highestBallot=topBallot;
                leader=(Some(topBallot,topProcess));
                trigger (BLE_Leader (topProcess, topBallot) -> ble );
                println(s"checkLeader: leader: $leader topProcess: $topProcess , topBallot: $topBallot to ble");
            }
        }
  }

  ctrl uponEvent {
    case _: Start =>  {
      startTimer(period);
    }
  }

  timer uponEvent {
    case CheckTimeout(_) => {
    if( (ballots.size+1) >= (topology.size / 2) ){
        checkLeader();
    }
     println("timeout");
     ballots==Map.empty;
     round +=1;
     topology.foreach{ p=> 
          if(p!=self){
               trigger (PL_Send(p, HeartbeatReq(round,highestBallot)) -> pl );
          }  
      }
      startTimer(period);
 
  }
  }

  pl uponEvent {
    case PL_Deliver(src, HeartbeatReq(r, hb)) => {
         if (hb > highestBallot){
             highestBallot=hb;
         }
         trigger (PL_Send(src, HeartbeatResp(r,ballot)) -> pl );
    }
    case PL_Deliver(src, HeartbeatResp(r, b)) => {
         if (r==round){
            ballots += ((src,b));
         }else{
            period += delta; 
         }
    }
  }
}
