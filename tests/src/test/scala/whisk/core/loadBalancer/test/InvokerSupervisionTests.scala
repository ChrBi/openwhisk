/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package whisk.core.loadBalancer.test

import akka.actor.{ActorRef, ActorRefFactory, ActorSystem, FSM}
import akka.actor.FSM.{CurrentState, SubscribeTransitionCallBack, Transition}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestFSMRef, TestKit, TestProbe}
import akka.util.Timeout
import common.{LoggedFunction, StreamLogging}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatest.junit.JUnitRunner
import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.connector.{ActivationMessage, PingMessage}
import whisk.core.connector.test.TestConnector
import whisk.core.entitlement.Privilege
import whisk.core.entity.ActivationId.ActivationIdGenerator
import whisk.core.entity.{ControllerInstanceId, _}
import whisk.core.loadBalancer._
import whisk.core.loadBalancer.InvokerState._
import whisk.utils.retry

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class InvokerSupervisionTests
    extends TestKit(ActorSystem("InvokerSupervision"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockFactory
    with StreamLogging {

  val config = new WhiskConfig(ExecManifest.requiredProperties)

  ExecManifest.initialize(config)

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val timeout = Timeout(5.seconds)

  /** Imitates a StateTimeout in the FSM */
  def timeout(actor: ActorRef) = actor ! FSM.StateTimeout

  /** Queries all invokers for their state */
  def allStates(pool: ActorRef) =
    Await.result(pool.ask(GetStatus).mapTo[IndexedSeq[(InvokerInstanceId, InvokerState)]], timeout.duration)

  /** Helper to generate a list of (InstanceId, InvokerState) */
  def zipWithInstance(list: IndexedSeq[InvokerState]) = list.zipWithIndex.map {
    case (state, index) => new InvokerHealth(InvokerInstanceId(index, "", 0), state)
  }

  val pC = new TestConnector("pingFeedTtest", 4, false) {}

  behavior of "InvokerPool"

  it should "successfully create invokers in its pool on ping and keep track of statechanges" in {
    val invoker5 = TestProbe()
    val invoker2 = TestProbe()

    val invoker5Instance = InvokerInstanceId(5, "", 0)
    val invoker2Instance = InvokerInstanceId(2, "", 0)

    val children = mutable.Queue(invoker5.ref, invoker2.ref)
    val childFactory = (f: ActorRefFactory, instance: InvokerInstanceId) => children.dequeue()

    val sendActivationToInvoker = stubFunction[ActivationMessage, InvokerInstanceId, Future[Unit]]
    val supervisor = system.actorOf(InvokerPool.props(childFactory, sendActivationToInvoker, pC))

    within(timeout.duration) {
      // create first invoker
      val ping0 = PingMessage(invoker5Instance)
      supervisor ! ping0
      invoker5.expectMsgType[SubscribeTransitionCallBack] // subscribe to the actor
      invoker5.expectMsg(ping0)

      invoker5.send(supervisor, CurrentState(invoker5.ref, Healthy))
      allStates(supervisor) shouldBe zipWithInstance(IndexedSeq(Offline, Offline, Offline, Offline, Offline, Healthy))

      // create second invoker
      val ping1 = PingMessage(invoker2Instance)
      supervisor ! ping1
      invoker2.expectMsgType[SubscribeTransitionCallBack]
      invoker2.expectMsg(ping1)

      invoker2.send(supervisor, CurrentState(invoker2.ref, Healthy))
      allStates(supervisor) shouldBe zipWithInstance(IndexedSeq(Offline, Offline, Healthy, Offline, Offline, Healthy))

      // ping the first invoker again
      supervisor ! ping0
      invoker5.expectMsg(ping0)

      allStates(supervisor) shouldBe zipWithInstance(IndexedSeq(Offline, Offline, Healthy, Offline, Offline, Healthy))

      // one invoker goes offline
      invoker2.send(supervisor, Transition(invoker2.ref, Healthy, Offline))
      allStates(supervisor) shouldBe zipWithInstance(IndexedSeq(Offline, Offline, Offline, Offline, Offline, Healthy))
    }
  }

  it should "forward the ActivationResult to the appropriate invoker" in {
    val invoker = TestProbe()
    val invokerInstance = InvokerInstanceId(0, "", 0)
    val invokerName = s"invoker${invokerInstance.toInt}"
    val childFactory = (f: ActorRefFactory, instance: InvokerInstanceId) => invoker.ref
    val sendActivationToInvoker = stubFunction[ActivationMessage, InvokerInstanceId, Future[Unit]]

    val supervisor = system.actorOf(InvokerPool.props(childFactory, sendActivationToInvoker, pC))

    within(timeout.duration) {
      // Create one invoker
      val ping0 = PingMessage(invokerInstance)
      supervisor ! ping0
      invoker.expectMsgType[SubscribeTransitionCallBack] // subscribe to the actor
      invoker.expectMsg(ping0)
      invoker.send(supervisor, CurrentState(invoker.ref, Healthy))
      allStates(supervisor) shouldBe zipWithInstance(IndexedSeq(Healthy))

      // Send message and expect receive in invoker
      val msg = InvocationFinishedMessage(invokerInstance, InvocationFinishedResult.Success)
      supervisor ! msg
      invoker.expectMsg(msg)
    }
  }

  it should "forward an ActivationMessage to the sendActivation-Method" in {
    val invoker = TestProbe()
    val invokerInstance = InvokerInstanceId(0, "", 0)
    val invokerName = s"invoker${invokerInstance.toInt}"
    val childFactory = (f: ActorRefFactory, instance: InvokerInstanceId) => invoker.ref

    val sendActivationToInvoker = LoggedFunction { (a: ActivationMessage, b: InvokerInstanceId) =>
      Future.successful(())
    }

    val supervisor = system.actorOf(InvokerPool.props(childFactory, sendActivationToInvoker, pC))

    // Send ActivationMessage to InvokerPool
    val uuid = UUID()
    val activationMessage = ActivationMessage(
      transid = TransactionId.invokerHealth,
      action = FullyQualifiedEntityName(EntityPath("whisk.system/utils"), EntityName("date")),
      revision = DocRevision.empty,
      user = Identity(
        Subject("unhealthyInvokerCheck"),
        Namespace(EntityName("unhealthyInvokerCheck"), uuid),
        BasicAuthenticationAuthKey(uuid, Secret()),
        Set[Privilege]()),
      activationId = new ActivationIdGenerator {}.make(),
      rootControllerIndex = ControllerInstanceId("0", "", 0),
      blocking = false,
      content = None)
    val msg = ActivationRequest(activationMessage, invokerInstance)

    supervisor ! msg

    // Verify, that MessageProducer will receive a call to send the message
    retry(sendActivationToInvoker.calls should have size 1, N = 3, waitBeforeRetry = Some(500.milliseconds))
  }

  behavior of "InvokerActor"

  // unHealthy -> offline
  // offline -> unhealthy
  it should "start unhealthy, go offline if the state times out and goes unhealthy on a successful ping again" in {
    val pool = TestProbe()
    val invoker = pool.system.actorOf(InvokerActor.props(InvokerInstanceId(0, "", 0), ControllerInstanceId("0", "", 0)))

    within(timeout.duration) {
      pool.send(invoker, SubscribeTransitionCallBack(pool.ref))
      pool.expectMsg(CurrentState(invoker, Unhealthy))
      timeout(invoker)
      pool.expectMsg(Transition(invoker, Unhealthy, Offline))

      invoker ! PingMessage(InvokerInstanceId(0, "", 0))
      pool.expectMsg(Transition(invoker, Offline, Unhealthy))
    }
  }

  // unhealthy -> healthy -> unhealthy -> healthy
  it should "goto healthy again, if unhealthy and error buffer has enough successful invocations" in {
    val pool = TestProbe()
    val invoker = pool.system.actorOf(InvokerActor.props(InvokerInstanceId(0, "", 0), ControllerInstanceId("0", "", 0)))

    within(timeout.duration) {
      pool.send(invoker, SubscribeTransitionCallBack(pool.ref))
      pool.expectMsg(CurrentState(invoker, Unhealthy))

      (1 to InvokerActor.bufferSize).foreach { _ =>
        invoker ! InvocationFinishedMessage(InvokerInstanceId(0, "", 0), InvocationFinishedResult.Success)
      }
      pool.expectMsg(Transition(invoker, Unhealthy, Healthy))

      // Fill buffer with errors
      (1 to InvokerActor.bufferSize).foreach { _ =>
        invoker ! InvocationFinishedMessage(InvokerInstanceId(0, "", 0), InvocationFinishedResult.SystemError)
      }
      pool.expectMsg(Transition(invoker, Healthy, Unhealthy))

      // Fill buffer with successful invocations to become healthy again (one below errorTolerance)
      (1 to InvokerActor.bufferSize - InvokerActor.bufferErrorTolerance).foreach { _ =>
        invoker ! InvocationFinishedMessage(InvokerInstanceId(0, "", 0), InvocationFinishedResult.Success)
      }
      pool.expectMsg(Transition(invoker, Unhealthy, Healthy))
    }
  }

  // unhealthy -> healthy -> overloaded -> healthy
  it should "goto healthy again, if overloaded and error buffer has enough successful invocations" in {
    val pool = TestProbe()
    val invoker = pool.system.actorOf(InvokerActor.props(InvokerInstanceId(0, "", 0), ControllerInstanceId("0", "", 0)))

    within(timeout.duration) {
      pool.send(invoker, SubscribeTransitionCallBack(pool.ref))
      pool.expectMsg(CurrentState(invoker, Unhealthy))

      (1 to InvokerActor.bufferSize).foreach { _ =>
        invoker ! InvocationFinishedMessage(InvokerInstanceId(0, "", 0), InvocationFinishedResult.Success)
      }
      pool.expectMsg(Transition(invoker, Unhealthy, Healthy))

      // Fill buffer with timeouts
      (1 to InvokerActor.bufferSize).foreach { _ =>
        invoker ! InvocationFinishedMessage(InvokerInstanceId(0, "", 0), InvocationFinishedResult.Timeout)
      }
      pool.expectMsg(Transition(invoker, Healthy, Unresponsive))

      // Fill buffer with successful invocations to become healthy again (one below errorTolerance)
      (1 to InvokerActor.bufferSize - InvokerActor.bufferErrorTolerance).foreach { _ =>
        invoker ! InvocationFinishedMessage(InvokerInstanceId(0, "", 0), InvocationFinishedResult.Success)
      }
      pool.expectMsg(Transition(invoker, Unresponsive, Healthy))
    }
  }

  // unhealthy -> offline
  // offline -> unhealthy
  it should "go offline when unhealthy, if the state times out and go unhealthy on a successful ping again" in {
    val pool = TestProbe()
    val invoker = pool.system.actorOf(InvokerActor.props(InvokerInstanceId(0, "", 0), ControllerInstanceId("0", "", 0)))

    within(timeout.duration) {
      pool.send(invoker, SubscribeTransitionCallBack(pool.ref))
      pool.expectMsg(CurrentState(invoker, Unhealthy))

      timeout(invoker)
      pool.expectMsg(Transition(invoker, Unhealthy, Offline))

      invoker ! PingMessage(InvokerInstanceId(0, "", 0))
      pool.expectMsg(Transition(invoker, Offline, Unhealthy))
    }
  }

  it should "start timer to send testactions when unhealthy" in {
    val invoker = TestFSMRef(new InvokerActor(InvokerInstanceId(0, "", 0), ControllerInstanceId("0", "", 0)))
    invoker.stateName shouldBe Unhealthy

    invoker.isTimerActive(InvokerActor.timerName) shouldBe true

    // Fill buffer with successful invocations to become healthy again (one below errorTolerance)
    (1 to InvokerActor.bufferSize - InvokerActor.bufferErrorTolerance).foreach { _ =>
      invoker ! InvocationFinishedMessage(InvokerInstanceId(0, "", 0), InvocationFinishedResult.Success)
    }
    invoker.stateName shouldBe Healthy

    invoker.isTimerActive(InvokerActor.timerName) shouldBe false
  }

  it should "initially store invoker status with its full id - instance/uniqueName/displayedName" in {

    val invoker0 = TestProbe()
    val children = mutable.Queue(invoker0.ref)
    val childFactory = (f: ActorRefFactory, instance: InvokerInstanceId) => children.dequeue()

    val sendActivationToInvoker = stubFunction[ActivationMessage, InvokerInstanceId, Future[RecordMetadata]]
    val supervisor = system.actorOf(InvokerPool.props(childFactory, sendActivationToInvoker, pC))

    val invokerInstance = InvokerInstanceId(0, Some("10.x.x.x"), Some("invoker-xyz"))

    within(timeout.duration) {

      val ping = PingMessage(invokerInstance)

      supervisor ! ping

      invoker0.expectMsgType[SubscribeTransitionCallBack]
      invoker0.expectMsg(ping)

      allStates(supervisor) shouldBe IndexedSeq(new InvokerHealth(invokerInstance, Offline))
    }
  }

  it should "update the invoker instance id after it was restarted" in {
    val invoker0 = TestProbe()
    val children = mutable.Queue(invoker0.ref)
    val childFactory = (f: ActorRefFactory, instance: InvokerInstanceId) => children.dequeue()

    val sendActivationToInvoker = stubFunction[ActivationMessage, InvokerInstanceId, Future[RecordMetadata]]
    val supervisor = system.actorOf(InvokerPool.props(childFactory, sendActivationToInvoker, pC))

    val invokerInstance = InvokerInstanceId(0, Some("10.x.x.x"), Some("invoker-xyz"))

    val invokerAfterRestart = InvokerInstanceId(0, Some("10.x.x.x"), Some("invoker-zyx"))

    within(timeout.duration) {
      val ping = PingMessage(invokerInstance)

      supervisor ! ping

      invoker0.expectMsgType[SubscribeTransitionCallBack]
      invoker0.expectMsg(ping)

      invoker0.send(supervisor, CurrentState(invoker0.ref, Unhealthy))

      val newPing = PingMessage(invokerAfterRestart)

      supervisor ! newPing

      allStates(supervisor) shouldBe IndexedSeq(new InvokerHealth(invokerAfterRestart, Unhealthy))
    }
  }
}
