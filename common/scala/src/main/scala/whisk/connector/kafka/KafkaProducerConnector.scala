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

import akka.actor.ActorSystem
import akka.kafka.internal.OwKafkaProducer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import whisk.common.{Counter, Logging}
import whisk.core.connector.{Message, MessageProducer}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class KafkaProducerConnector()(implicit actorSystem: ActorSystem, logging: Logging) extends MessageProducer {

  private implicit val executionContext = actorSystem.dispatchers.lookup("akka.kafka.default-dispatcher")
  private implicit val materializer = ActorMaterializer()

  override def sentCount() = sentCounter.cur

  val source =
    Source.queue[(ProducerRecord[String, String], Promise[RecordMetadata])](Int.MaxValue, OverflowStrategy.dropNew)
  val producerSettings = ProducerSettings(actorSystem, new StringSerializer, new StringSerializer)

  val kafkaProducer: SourceQueueWithComplete[(ProducerRecord[String, String], Promise[RecordMetadata])] = source
    .map { case (msg, prom) => ProducerMessage.Message(msg, prom) }
    .map { m =>
      println(m)
      m
    }
    .via(OwKafkaProducer.flow(producerSettings))
    .map { m =>
      println(m)
      m
    }
    .map(result => result.message.passThrough.success(result.metadata))
    .recover { case t => println(t) }
    .to(Sink.ignore)
    .run()

  /** Sends msg to topic. This is an asynchronous operation. */
  override def send(topic: String, msg: Message, retry: Int = 2): Future[RecordMetadata] = {
    implicit val transid = msg.transid
    val record = new ProducerRecord[String, String](topic, "messages", msg.serialize)

    logging.debug(this, s"sending to topic '$topic' msg '$msg'")
    val produced = Promise[RecordMetadata]()

    kafkaProducer
      .offer((record, produced))
      .map {
        case QueueOfferResult.Failure(t) => produced.tryFailure(t)
        case q                           => println(q)
      }
      .recover {
        case t => produced.tryFailure(t)
      }

    produced.future.andThen {
      case Success(status) =>
        logging.debug(this, s"sent message: ${status.topic()}[${status.partition()}][${status.offset()}]")
        sentCounter.next()
      case Failure(t) =>
        logging.error(this, s"sending message on topic '$topic' failed: ${t.getMessage}")
    }
  }

  /** Closes producer. */
  override def close() = {
    kafkaProducer.complete()
  }

  private val sentCounter = new Counter()
}
