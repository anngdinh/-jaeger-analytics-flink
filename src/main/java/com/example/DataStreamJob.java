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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringDeserializer;

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

    // *************************************************************************
   // PROGRAM
   // *************************************************************************
   public static void main(String[] args) throws Exception {
	final ParameterTool params = ParameterTool.fromArgs(args);
	// set up the execution environment
	final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	// make parameters available in the web interface
	env.getConfig().setGlobalJobParameters(params);
	// get input data
	DataSet<String> text = env.readTextFile(params.get("input"));
	DataSet<Tuple2<String, Integer>> counts =
	// split up the lines in pairs (2-tuples) containing: (word,1)
	text.flatMap(new Tokenizer())
	// group by the tuple field "0" and sum up tuple field "1"
	.groupBy(0)
	.sum(1);
	// emit result
	if (params.has("output")) {
	   counts.writeAsCsv(params.get("output"), "\n", " ");
	   // execute program
	   env.execute("My Example");
	} else {
	   System.out.println("Printing result to stdout. Use --output to specify output path.");
	   counts.print();
	}
 }
 
 // *************************************************************************
 // USER FUNCTIONS
 // *************************************************************************
 public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
	public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
	   // normalize and split the line
	   String[] tokens = value.toLowerCase().split("\\W+");
	   // emit the pairs
	   for (String token : tokens) {
		  if (token.length() > 0) {
			 out.collect(new Tuple2<>(token, 1));
		  }
	   }
	}
 }
}
