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

package com.example;

import com.example.deserializer.SpanDeserializer;
import com.example.model.Dependency;
import com.example.model.SpanModel;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {
    private static Map<String, String> traceIdToService = new HashMap<String, String>();
    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<SpanModel> source = KafkaSource.<SpanModel>builder()
                .setBootstrapServers("103.245.251.90:9093")
                .setTopics("tracing-dev-stg")
                .setGroupId("dev-stg-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                // .setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(new SpanDeserializer())

                .setProperty("security.protocol", "SSL")
                .setProperty("ssl.keystore.location", "/home/annd2/Documents/SPM/jaeger-analytics/admin.key")
                .setProperty("ssl.keystore.password", "OqLxuB0UeJY9FnNfVohVVzG5UhgdKbG5")
                .setProperty("ssl.key.password", "OqLxuB0UeJY9FnNfVohVVzG5UhgdKbG5")

                .setProperty("ssl.truststore.location", "/home/annd2/Documents/SPM/jaeger-analytics/VNG.trust")
                .setProperty("ssl.truststore.password", "JAnlbz9zqAEHK9870IVLwfbV7ASk081k")
                .setProperty("ssl.endpoint.identification.algorithm", "")

                .build();

        DataStream<SpanModel> temp = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

//        DataStreamJob.printPairTrace(temp);

        DataStream<Tuple3<String, String, Integer>> dataStream = temp
                .flatMap(new NYCEnrichment())
                .keyBy(value -> (value.f0 + value.f1))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(2);
        dataStream.print();



        env.execute("Flink Java API Skeleton");
    }



    public static void printPairTrace(DataStream<SpanModel> temp) {
        DataStream<Tuple3<String, String, Integer>> pairDependency = temp
                .map(new MapFunction<SpanModel, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> map(SpanModel spanModel) throws Exception {
                        String serviceName = spanModel.getProcess().getServiceName();
                        traceIdToService.put(spanModel.getTraceId(), serviceName);
                        String refTrace = null;
                        if (spanModel.getReferences() != null){
                            refTrace = spanModel.getReferences().get(0).get("traceId");
                            if (traceIdToService.get(refTrace) != null) {
                                String refService = traceIdToService.get(refTrace);
                                return new Tuple3<>(serviceName, refService,1);
                            }
                        }
                        return new Tuple3<>(serviceName, refTrace,1);
                    }
                });
        pairDependency.print();
    }


    public static class NYCEnrichment implements FlatMapFunction<SpanModel, Tuple3<String, String, Integer>> {
        @Override
        public void flatMap(SpanModel spanModel, Collector<Tuple3<String, String, Integer>> out) throws Exception {

            String serviceName = spanModel.getProcess().getServiceName();
            traceIdToService.put(spanModel.getTraceId(), serviceName);
            String refTrace = null;
            if (spanModel.getReferences() != null){
                refTrace = spanModel.getReferences().get(0).get("traceId");
                if (traceIdToService.get(refTrace) != null) {
                    String refService = traceIdToService.get(refTrace);
                    out.collect(new Tuple3<String, String, Integer>(serviceName, refService, 1));
                }
            }

//            else {
//                out.collect(new Tuple3<String, String, Integer>(serviceName, refTrace, 1));
//            } 
//            ................................

        }
    }

}
