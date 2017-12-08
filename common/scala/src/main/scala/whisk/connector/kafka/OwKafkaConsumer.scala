package whisk.connector.kafka

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext

object OwKafkaConsumer {

  def bufferedSource(kafkaHosts: String, group: String, topic: String, maxBatchSize: Int)(
    implicit actorSystem: ActorSystem,
    executionContext: ExecutionContext) = {

    batchedSouce(kafkaHosts, group, topic, maxBatchSize).mapConcat(identity)
  }

  def batchedSouce(kafkaHosts: String, group: String, topic: String, maxBatchSize: Int)(
    implicit actorSystem: ActorSystem,
    executionContext: ExecutionContext) = {
    val settings = ConsumerSettings(actorSystem, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers(kafkaHosts)
      .withGroupId(group)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    Consumer
      .committableSource(settings, Subscriptions.topics(topic))
      .batch(maxBatchSize, Queue(_))(_ :+ _)
      .mapAsync(4) { msgs =>
        msgs
          .foldLeft(CommittableOffsetBatch.empty)((batch, msg) => batch.updated(msg.committableOffset))
          .commitScaladsl()
          .map { _ =>
            msgs.map(_.record)
          }
      }
  }

}
