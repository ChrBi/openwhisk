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

  def bufferedSource(group: String, topic: String, maxBatchSize: Int)(
    implicit actorSystem: ActorSystem): Source[String, NotUsed] = {

    batchedSouce(group, topic, maxBatchSize).mapConcat(identity)
  }

  def batchedSouce(group: String, topic: String, maxBatchSize: Int)(
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
