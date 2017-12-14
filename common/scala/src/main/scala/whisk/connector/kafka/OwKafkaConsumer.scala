package whisk.connector.kafka

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{RestartSource, Source}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.duration._
import scala.collection.immutable.Queue

object OwKafkaConsumer {

  def bufferedSource(kafkaHosts: String, group: String, topic: String, maxBatchSize: Int)(
    implicit actorSystem: ActorSystem): Source[String, NotUsed] = {

    batchedSouce(kafkaHosts, group, topic, maxBatchSize).mapConcat(identity)
  }

  def batchedSouce(kafkaHosts: String, group: String, topic: String, maxBatchSize: Int)(
    implicit actorSystem: ActorSystem): Source[Queue[String], NotUsed] = {
    implicit val ec = actorSystem.dispatcher
    val settings = ConsumerSettings(actorSystem, new ByteArrayDeserializer, new StringDeserializer)
      .withGroupId(group)
      .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxBatchSize.toString)

    RestartSource.withBackoff(minBackoff = 500.milliseconds, maxBackoff = 30.seconds, randomFactor = 0.1) { () =>
      Consumer
        .committableSource(settings, Subscriptions.topics(topic))
        .batch(maxBatchSize, Queue(_))(_ :+ _)
        .mapAsync(4) { msgs =>
          msgs
            .foldLeft(CommittableOffsetBatch.empty)((batch, msg) => batch.updated(msg.committableOffset))
            .commitScaladsl()
            .map { _ =>
              msgs.map(_.record.value)
            }
        }
    }
  }

}
