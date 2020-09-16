/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.moye

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import java.util.Properties

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val textStream = env.socketTextStream("10.129.15.220", 9000, '\n')

    val windowWordCount  = textStream
      .flatMap(line => line.split("\\s"))
      .map(word => (word, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    windowWordCount
      .addSink(new SinkFunction[(String, Int)] {
        override def invoke(in: (String, Int)): Unit = {
          sendMessage("(%s,%d)".format(in._1, in._2))
        }
      })
      .setParallelism(1)

    // execute program
    env.execute("Socket Window WordCount")
  }

  def sendMessage(message: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "10.129.15.179:9092,10.129.15.229:9092,10.129.15.76:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val topic = "test_topic"
    val producer = new KafkaProducer[String, String](props)
    val jsonData = "{\"message\": \"%s\"}".format(message)
    val data = new ProducerRecord[String, String](topic, jsonData)
    producer.send(data)
    producer.close()
  }
}
