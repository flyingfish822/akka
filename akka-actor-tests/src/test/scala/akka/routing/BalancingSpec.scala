/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.routing

import language.postfixOps
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.{ Props, Actor }
import akka.testkit.{ TestLatch, ImplicitSender, DefaultTimeout, AkkaSpec }
import akka.pattern.ask

object BalancingSpec {
  val counter = new AtomicInteger(1)

  class Worker(latch: TestLatch) extends Actor {
    lazy val id = counter.getAndIncrement()
    def receive = {
      case msg ⇒
        if (id == 1) Thread.sleep(10) // dispatch to other routees
        else Await.ready(latch, 1.minute)
        sender ! id
    }
  }
}

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class BalancingSpec extends AkkaSpec with DefaultTimeout with ImplicitSender {
  import BalancingSpec._

  "balancing pool" must {

    "deliver messages in a balancing fashion" in {
      val poolSize = 10
      val iterationCount = 100

      val latch = TestLatch(1)
      val pool = system.actorOf(BalancingPool(poolSize).props(routeeProps =
        Props(classOf[Worker], latch)), name = "balancingPool")

      for (i ← 1 to iterationCount) {
        pool ! "hit-" + i
      }

      // all but one worker are blocked
      val replies1 = receiveN(iterationCount - poolSize + 1)
      expectNoMsg(1.second)
      // all replies from the unblocked worker so far
      replies1.toSet should be(Set(1))

      latch.countDown()
      val replies2 = receiveN(poolSize - 1)
      // the remaining replies come from the blocked 
      replies2.toSet should be((2 to poolSize).toSet)
    }

  }
}
