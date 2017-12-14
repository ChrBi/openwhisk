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

package whisk.core.connector

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import whisk.common.Logging
import whisk.core.WhiskConfig
import whisk.spi.Spi

import scala.concurrent.ExecutionContext

/**
 * An Spi for providing Messaging implementations.
 */
trait MessagingProvider extends Spi {
  def getConsumer(kafkaHosts: String, group: String, topic: String, maxBatchSize: Int)(
    implicit actorSystem: ActorSystem): Source[String, _]

  def getProducer(config: WhiskConfig, ec: ExecutionContext)(implicit actorSystem: ActorSystem,
                                                             logging: Logging): MessageProducer

  def ensureTopic(config: WhiskConfig, topic: String, topicConfig: String)(implicit logging: Logging): Boolean
}
