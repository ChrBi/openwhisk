package akka.kafka.internal

import java.util.concurrent.TimeUnit

import akka.kafka.ProducerMessage.{Message, Result}
import akka.stream._
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.stage._
import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}
import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.kafka.ProducerSettings
import akka.stream.scaladsl.Flow
import org.apache.kafka.common.errors.RetriableException

object OwKafkaProducer {
  def flow[K, V, PassThrough](
    settings: ProducerSettings[K, V]): Flow[Message[K, V, PassThrough], Result[K, V, PassThrough], NotUsed] = {
    val flow = Flow
      .fromGraph(
        new OwProducerStage[K, V, PassThrough](
          settings.closeTimeout,
          closeProducerOnStop = true,
          () => settings.createKafkaProducer()))
      .mapAsync(settings.parallelism)(identity)

    if (settings.dispatcher.isEmpty) flow
    else flow.withAttributes(ActorAttributes.dispatcher(settings.dispatcher))
  }
}

/**
 * INTERNAL API
 */
private[kafka] class OwProducerStage[K, V, P](closeTimeout: FiniteDuration,
                                              closeProducerOnStop: Boolean,
                                              producerProvider: () => Producer[K, V])
    extends GraphStage[FlowShape[Message[K, V, P], Future[Result[K, V, P]]]] {

  private val in = Inlet[Message[K, V, P]]("messages")
  private val out = Outlet[Future[Result[K, V, P]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes) = {
    val producer = producerProvider()
    val logic = new GraphStageLogic(shape) with StageLogging {
      lazy val decider =
        inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)
      val awaitingConfirmation = new AtomicInteger(0)
      @volatile var inIsClosed = false

      var completionState: Option[Try[Unit]] = None

      override protected def logSource: Class[_] = classOf[OwProducerStage[K, V, P]]

      def checkForCompletion() = {
        if (isClosed(in) && awaitingConfirmation.get == 0) {
          completionState match {
            case Some(Success(_))  => completeStage()
            case Some(Failure(ex)) => failStage(ex)
            case None              => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
          }
        }
      }

      val checkForCompletionCB = getAsyncCallback[Unit] { _ =>
        checkForCompletion()
      }

      val failStageCb = getAsyncCallback[Throwable] { ex =>
        failStage(ex)
      }

      setHandler(out, new OutHandler {
        override def onPull() = {
          tryPull(in)
        }
      })

      setHandler(
        in,
        new InHandler {

          implicit val ec = akka.dispatch.ExecutionContexts.sameThreadExecutionContext
          def sendMessage(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
            val r = Promise[RecordMetadata]
            producer.send(record, new Callback {
              override def onCompletion(metadata: RecordMetadata, exception: Exception) = {
                if (exception == null) {
                  r.success(metadata)
                } else {
                  r.failure(exception)
                }
              }
            })

            r.future.recoverWith {
              case e: RetriableException =>
                println(s"retrying because of $e")
                sendMessage(record)
              case t =>
                println(s"not retrying because of $t")
                Future.failed(t)
            }
          }

          override def onPush() = {
            val msg = grab(in)

            val result = sendMessage(msg.record)
              .map(meta => Result(meta, msg))
              .andThen {
                case _ =>
                  if (awaitingConfirmation.decrementAndGet() == 0 && inIsClosed)
                    checkForCompletionCB.invoke(())
              }

            awaitingConfirmation.incrementAndGet()
            push(out, result)
          }

          override def onUpstreamFinish() = {
            inIsClosed = true
            completionState = Some(Success(()))
            checkForCompletion()
          }

          override def onUpstreamFailure(ex: Throwable) = {
            inIsClosed = true
            completionState = Some(Failure(ex))
            checkForCompletion()
          }
        })

      override def postStop() = {
        log.debug("Stage completed")

        if (closeProducerOnStop) {
          try {
            // we do not have to check if producer was already closed in send-callback as `flush()` and `close()` are effectively no-ops in this case
            producer.flush()
            producer.close(closeTimeout.toMillis, TimeUnit.MILLISECONDS)
            log.debug("Producer closed")
          } catch {
            case NonFatal(ex) => log.error(ex, "Problem occurred during producer close")
          }
        }

        super.postStop()
      }
    }
    logic
  }
}
