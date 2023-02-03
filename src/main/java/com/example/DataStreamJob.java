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
import com.example.model.SpanModel;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
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
import java.time.LocalDate;
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
    private static MaxSizeHashMap<String, String> spanIdToService = new MaxSizeHashMap<String, String>(1000);
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String pathConfigFile = parameters.getRequired("config-file");

        Properties prop = new Properties();
        try (InputStream input = new FileInputStream(pathConfigFile)) {
            // load a properties file
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<SpanModel> source = KafkaSource.<SpanModel>builder()
                .setBootstrapServers(prop.getProperty("kafka.bootstrap.servers"))
                .setTopics(prop.getProperty("kafka.topic"))
                .setGroupId(prop.getProperty("kafka.groupid"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SpanDeserializer())

                .setProperty("security.protocol", prop.getProperty("kafka.security.protocol"))
                .setProperty("ssl.keystore.location", prop.getProperty("kafka.ssl.keystore.location"))
                .setProperty("ssl.keystore.password", prop.getProperty("kafka.ssl.keystore.password"))
                .setProperty("ssl.key.password", prop.getProperty("kafka.ssl.key.password"))

                .setProperty("ssl.truststore.location", prop.getProperty("kafka.ssl.truststore.location"))
                .setProperty("ssl.truststore.password", prop.getProperty("kafka.ssl.truststore.password"))
                .setProperty("ssl.endpoint.identification.algorithm", prop.getProperty("kafka.ssl.endpoint.identification.algorithm"))

                .build();

        DataStream<SpanModel> temp = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<Tuple3<String, String, Integer>> dataStream = temp
                .flatMap(new FlatTraceData())
                .keyBy(value -> (value.f0 + value.f1))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(2);

        DataStream<ArrayList<Tuple3<String, String, Integer>>> arr = dataStream
                .flatMap(new FlatTraceDataArray())
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce ((ReduceFunction<ArrayList<Tuple3<String, String, Integer>>>) (array, value2) -> {
                    array.add(value2.get(0));
                    return array;
                });

        arr.print();

        arr.sinkTo(
                new Elasticsearch7SinkBuilder<ArrayList<Tuple3<String, String, Integer>>>()
//                        .setBulkFlushInterval(5000)
                        .setBulkFlushMaxActions(1) // Instructs the sink to emit after every element, otherwise they would be buffered
                        .setHosts(new HttpHost(prop.getProperty("es.host"), 9200, "http"))
                        .setConnectionUsername(prop.getProperty("es.username"))
                        .setConnectionPassword(prop.getProperty("es.password"))
                        .setEmitter(
                                (element, context, indexer) ->
//                                        indexer.add(createCloneSpark(element.f0, element.f1, element.f2)))
                                        indexer.add(createCloneSparkArray(element)))
                        .build());

        env.execute("Flink Java API Skeleton");
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
                else {
                    System.out.println("\n\nERR: Not handle null span reference !\n");
                }
            }
        }
    }

    public static class FlatTraceDataArray implements FlatMapFunction<Tuple3<String, String, Integer>, ArrayList<Tuple3<String, String, Integer>>> {
        @Override
        public void flatMap(Tuple3<String, String, Integer> tuple3, Collector<ArrayList<Tuple3<String, String, Integer>>> out) throws Exception {
            ArrayList<Tuple3<String, String, Integer>> arr = new ArrayList<Tuple3<String, String, Integer>>();
            arr.add(tuple3);
            out.collect(arr);
        }
    }
    private static IndexRequest createCloneSparkArray(ArrayList<Tuple3<String, String, Integer>> arr) {
        ArrayList<Object> str = new ArrayList<Object>();

        for (int i = 0; i < arr.size(); i++) {
            Tuple3<String, String, Integer> tuple3 = arr.get(i);
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("parent", tuple3.f0);
            data.put("child", tuple3.f1);
            data.put("callCount", tuple3.f2);
            data.put("source", "jaeger");

            str.add(data);
        }

        Map<String, Object> json = new HashMap<String, Object>();
        json.put("dependencies", str);

        Instant currentTimeStamp = Instant.now();
        json.put("timestamp", currentTimeStamp);

        LocalDate currentDate = LocalDate.now();

        return Requests.indexRequest()
                .index("tracing-dev-stg-jaeger-dependencies-" + currentDate.toString())
                .source(json);
    }

}

// SpanModel ---- FlatTraceData (5s) ----> Tuple3<service, parentService, count: int> ----FlatTraceDataArray and ReduceFunction----> ArrayList<Tuple3<String, String, Integer>> ----> sinkToES

