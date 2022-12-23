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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.*;

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
    private static Map<String, String> spanIdToService = new HashMap<String, String>();
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        try (InputStream input = new FileInputStream("src/main/resources/config.properties")) {
            // load a properties file
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<SpanModel> source = KafkaSource.<SpanModel>builder()
                .setBootstrapServers(prop.getProperty("bootstrap.servers"))
                .setTopics(prop.getProperty("topic"))
                .setGroupId(prop.getProperty("groupid"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                // .setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(new SpanDeserializer())

                .setProperty("security.protocol", prop.getProperty("security.protocol"))
                .setProperty("ssl.keystore.location", prop.getProperty("ssl.keystore.location"))
                .setProperty("ssl.keystore.password", prop.getProperty("ssl.keystore.password"))
                .setProperty("ssl.key.password", prop.getProperty("ssl.key.password"))

                .setProperty("ssl.truststore.location", prop.getProperty("ssl.truststore.location"))
                .setProperty("ssl.truststore.password", prop.getProperty("ssl.truststore.password"))
                .setProperty("ssl.endpoint.identification.algorithm", prop.getProperty("ssl.endpoint.identification.algorithm"))

                .build();

        DataStream<SpanModel> temp = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

//         DataStreamJob.printPairTrace(temp);

        DataStream<Tuple3<String, String, Integer>> dataStream = temp
                .flatMap(new FlatTraceData())
                .keyBy(value -> (value.f0 + value.f1))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(2);
//         dataStream.print();


        dataStream.sinkTo(
                new Elasticsearch7SinkBuilder<Tuple3<String, String, Integer>>()
//                        .setBulkFlushInterval(5000)
                        .setBulkFlushMaxActions(1) // Instructs the sink to emit after every element, otherwise they would be buffered
                        .setHosts(new HttpHost(prop.getProperty("es.host"), 9200, "http"))
                        .setConnectionUsername(prop.getProperty("es.username"))
                        .setConnectionPassword(prop.getProperty("es.password"))
                        .setEmitter(
                                (element, context, indexer) ->
                                        indexer.add(createCloneSpark(element.f0, element.f1, element.f2)))
                        .build());

        env.execute("Flink Java API Skeleton");
    }


    private static IndexRequest createCloneSpark(String parent, String child, Integer callCount) {
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("parent", parent);
        data.put("child", child);
        data.put("callCount", callCount);
        data.put("source", "jaeger");

        ArrayList<Object> str = new ArrayList<Object>();
        str.add(data);
//        str.add(data);

        Map<String, Object> json = new HashMap<String, Object>();
        json.put("dependencies", str);

        Instant currentTimeStamp = Instant.now();
        json.put("timestamp", currentTimeStamp);



        return Requests.indexRequest()
                .index("tracing-dev-stg-jaeger-dependencies-2022-12-23")
                .source(json);


    }


    public static void printPairTrace(DataStream<SpanModel> temp) {
        DataStream<Tuple3<String, String, Integer>> pairDependency = temp
                .map(new MapFunction<SpanModel, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> map(SpanModel spanModel) throws Exception {
                        String serviceName = spanModel.getProcess().getServiceName();
                        spanIdToService.put(spanModel.getSpanId(), serviceName);
                        String refSpan = null;
                        if (spanModel.getReferences() != null){
                            refSpan = spanModel.getReferences().get(0).get("spanId");
                            if (spanIdToService.get(refSpan) != null) {
                                String refService = spanIdToService.get(refSpan);
                                // if (!serviceName.equals(refService)) System.out.print("\n*" + serviceName + "*\n*" + refService + "*\n");
                                // else System.out.print("\n-------------\n");
                                return new Tuple3<>(serviceName, refService,1);
                            }
                        }
                        // if (refSpan==null) System.out.print("null\n");
                        return new Tuple3<>(serviceName, refSpan,1);
                    }
                });
        pairDependency.print();
    }

    public static class FlatTraceData implements FlatMapFunction<SpanModel, Tuple3<String, String, Integer>> {
        @Override
        public void flatMap(SpanModel spanModel, Collector<Tuple3<String, String, Integer>> out) throws Exception {

            String serviceName = spanModel.getProcess().getServiceName();
            spanIdToService.put(spanModel.getSpanId(), serviceName);
            String refSpan = null;
            if (spanModel.getReferences() != null){
                refSpan = spanModel.getReferences().get(0).get("spanId");
                if (spanIdToService.get(refSpan) != null) {
                    String refService = spanIdToService.get(refSpan);
                    out.collect(new Tuple3<String, String, Integer>(serviceName, refService, 1));
                }
            }

//            else {
//                out.collect(new Tuple3<String, String, Integer>(serviceName, refSpan, 1));
//            }
//            ................................

        }
    }

}
