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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

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

    val servers = "10.129.15.179:9092,10.129.15.229:9092,10.129.15.76:9092"
    val topic = "test_topic"
    val myProducer = new FlinkKafkaProducer[(String, Int)](
      servers,
      topic,
      new KeyedSerializationSchema[(String, Int)] {
        override def serializeKey(t: (String, Int)): Array[Byte] = "".getBytes

        override def serializeValue(t: (String, Int)): Array[Byte] = "{\"%s\":%d}".format(t._1, t._2).getBytes

        override def getTargetTopic(t: (String, Int)): String = topic
      }
    )

    windowWordCount
      .addSink(myProducer)
      .setParallelism(1)

    // execute program
    env.execute("Socket Window WordCount")
  }
}
