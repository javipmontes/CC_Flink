package myflink;/*
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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Objects;

import static org.apache.commons.math3.util.FastMath.max;
import static org.apache.commons.math3.util.FastMath.min;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment

		final String file_path = "/home/can/IdeaProjects/CC_Flink/VehicleTelematics/data/sample-traffic-3xways.txt";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStreamSource<String> data = env.readTextFile(file_path);

		SingleOutputStreamOperator<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>> mapStream = data.
				map(new MapFunction<String, Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>>() {
					public Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> map(String in) throws Exception{
						String[] fieldArray = in.split(",");
						return new Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>(Long.parseLong(fieldArray[0]),
								Integer.parseInt(fieldArray[1]), Long.parseLong(fieldArray[2]), Integer.parseInt(fieldArray[3]),
								Integer.parseInt(fieldArray[4]), Integer.parseInt(fieldArray[5]), Integer.parseInt(fieldArray[6]),
								Long.parseLong(fieldArray[7]));
					}
				});
		DataStream<Tuple6<Long, Integer, Integer, Integer, Integer, Long>> speedFines= mapStream
				.filter(new FilterFunction<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>>() {
					@Override
					public boolean filter(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> in) throws Exception {
						return in.f2>90;
					}
				}).map(new MapFunction<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>, Tuple6<Long, Integer, Integer, Integer, Integer, Long>>() {
					@Override
					public Tuple6<Long, Integer, Integer, Integer, Integer, Long> map(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> input) throws Exception {
						return new Tuple6<Long, Integer, Integer, Integer, Integer, Long>(input.f0, input.f1, input.f3,
								input.f6, input.f5, input.f2);
					}
				});

		speedFines.writeAsCsv("results/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		// Detección de accidentes
		DataStream<Tuple7<Long, Long, Integer, Integer, Integer, Integer, Long>> accidentVehicles = mapStream.filter(
				new FilterFunction<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>>() {
					@Override
					public boolean filter(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> vehicleData) throws Exception {
						return vehicleData.f3 == 0;
					}
				}
		).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>>() {
										   @Override
										   public long extractAscendingTimestamp(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> vehicleData) {
											   return vehicleData.f0*1000;
										   }
									   }
		).keyBy(1).window(SlidingEventTimeWindows.of(Time.seconds(120), Time.seconds(30)))
				.apply(new DetectAccident());



		accidentVehicles.writeAsCsv("results/accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		DataStream<Tuple6<Long, Long, Integer, Integer, Integer, Double>> carsAvg = mapStream.filter(new FilterFunction<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>>() {
			@Override
			public boolean filter(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> in) throws Exception {
				return in.f6 <= 56 && in.f6 >= 52;
			}
		}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>>() {
			  @Override
			  public long extractAscendingTimestamp(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> vehicleData) {
				  return vehicleData.f0*1000;
			  }
		  }
		).keyBy(1).window(EventTimeSessionWindows.withGap(Time.seconds(60)))
				.aggregate(new AggregateFunction<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>,
						Tuple8<Long, Long, Integer, Integer, Integer, Long, Long, Integer>,
						Tuple6<Long, Long, Integer, Integer, Integer, Double>>() {

			@Override
			public Tuple8<Long, Long, Integer, Integer, Integer, Long, Long, Integer> createAccumulator() {
				return new Tuple8<Long, Long, Integer, Integer, Integer, Long, Long, Integer>(9223372036854775807L, 0L,
						0, 0, 0, 9223372036854775807L, 0L, 1);
			}

			@Override
			public Tuple8<Long, Long, Integer, Integer, Integer, Long, Long, Integer> add(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> value, Tuple8<Long, Long, Integer, Integer, Integer, Long, Long, Integer> keepValue) {
				keepValue.f0 = min(value.f0, keepValue.f0);
				keepValue.f1 = max(value.f0, keepValue.f1);
				keepValue.f2 = value.f1;
				keepValue.f3 = value.f3;
				keepValue.f4 = value.f5;
				keepValue.f5 = min(value.f7, keepValue.f5);
				keepValue.f6 = max(value.f7, keepValue.f6);
				if (value.f4 == 0 || value.f4 == 4) {
					keepValue.f7 = value.f4;
				}
				return keepValue;
			}

			@Override
			public Tuple6<Long, Long, Integer, Integer, Integer, Double> getResult(Tuple8<Long, Long, Integer, Integer, Integer, Long, Long, Integer> values) {
				if (values.f7 == 1 || values.f7 ==2 || values.f7 == 3) {
					double avgSpeed = (values.f6 - values.f5) / (double)(values.f1-values.f0)*2.237;
					return new Tuple6<Long, Long, Integer, Integer, Integer, Double>(
							values.f0, values.f1, values.f2, values.f3, values.f4, avgSpeed
					);
				}
				return null;
			}

			@Override
			public Tuple8<Long, Long, Integer, Integer, Integer, Long, Long, Integer> merge(Tuple8<Long, Long, Integer, Integer, Integer, Long, Long, Integer> keepValue, Tuple8<Long, Long, Integer, Integer, Integer, Long, Long, Integer> acc1) {
				keepValue.f0 = min(keepValue.f0, acc1.f0);
				keepValue.f1 = max(keepValue.f1, acc1.f1);
				keepValue.f5 = min(keepValue.f5, keepValue.f5);
				keepValue.f6 = max(keepValue.f6, keepValue.f6);
				if (acc1.f7 == 0 || acc1.f7 == 4) {
					keepValue.f7 = acc1.f7;
				}
				return keepValue;
			}
		}).filter(new FilterFunction<Tuple6<Long, Long, Integer, Integer, Integer, Double>>() {
					@Override
					public boolean filter(Tuple6<Long, Long, Integer, Integer, Integer, Double> speeds) throws Exception {
						return speeds.f5>60;
					}
				});
		carsAvg.writeAsCsv("results/avgFines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}


	public static class DetectAccident implements WindowFunction<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>, Tuple7<Long, Long, Integer, Integer, Integer, Integer, Long>, Tuple, TimeWindow> {
		public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>> input, Collector<Tuple7<Long, Long, Integer, Integer, Integer, Integer, Long>> out) throws Exception {
			Iterator<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>> iterator = input.iterator();
			Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> first = iterator.next();
			Long time1 = 0L;
			Long time2 = 0L;
			Integer vid = 0;
			Integer xWay = 0;
			Integer seg = 0;
			Integer dir = 0;
			Long pos = 0L;
			int i = 0;
			if (first !=null){
				time1 = first.f0;
				time2 = first.f0;
				vid = first.f1;
				xWay = first.f3;
				seg = first.f6;
				dir = first.f5;
				pos = first.f7;
			}
			while(iterator.hasNext()){
				Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> next = iterator.next();
				time1 = min(time1, next.f0);
				time2 = max(time2, next.f0);
				i += 1;
				if (Objects.equals(pos, next.f7) && i==3) {
					out.collect(new Tuple7<Long, Long, Integer, Integer, Integer, Integer, Long>(time1, time2, vid, xWay, seg, dir, pos));
				}
			}


		}
	}
}