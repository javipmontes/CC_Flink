
package myflink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;





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

		ParameterTool parameter = ParameterTool.fromArgs(args);

		final String input_path = parameter.get("input");

		final String output_path = parameter.get("output", "speedfines.csv");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> data = env.readTextFile(input_path);

		//Map function to separate the values of a tuple
		SingleOutputStreamOperator<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>> mapStream = data.
				map(new MapFunction<String, Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>>() {
					public Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> map(String in) throws Exception{
						String[] fieldArray = in.split(","); //split it when it finds a comma
						Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> out = new Tuple8(Long.parseLong(fieldArray[0]),
								Integer.parseInt(fieldArray[1]), Long.parseLong(fieldArray[2]), Integer.parseInt(fieldArray[3]),
								Integer.parseInt(fieldArray[4]), Integer.parseInt(fieldArray[5]), Integer.parseInt(fieldArray[6]),
								Long.parseLong(fieldArray[7]));

						return out; //You return the new tuple without commas
					}
				});

		//First functionality: Detects cars that overcome the speed limit of 90 mph.
		DataStream<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>> speeders= mapStream  //Concatenates the output of the map function with this filter
				.filter(new FilterFunction<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>>() { //Outputs those tuples for which the predicate is true
					@Override
					public boolean filter(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> in) throws Exception {
						return in.f2>90; //Returns the tuples with a speed higher than 90
					}
				});

		//Prepares the output tuples with a map function
		SingleOutputStreamOperator<Tuple6<Long, Integer, Integer, Integer, Integer, Long>> speedFines = speeders
				.map(new MapFunction<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>, Tuple6<Long, Integer, Integer, Integer, Integer, Long>>() {
					@Override
					public Tuple6<Long, Integer, Integer, Integer, Integer, Long> map(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> input) throws Exception { //Enters a tuple8 and exits a tuple6
						Tuple6<Long, Integer, Integer, Integer, Integer, Long> output = new Tuple6(input.f0, input.f1, input.f3,
								input.f6, input.f5, input.f2);
						return output; //Returns the tuple6 for the output of the SpeedRadar functionality
					}
				});

		//The result speedFines is written in a CSV with name given by the "output" parameter
		speedFines.writeAsCsv(output_path + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //Second functionality: AverageSpeedControl

		//Filter the tuples that are between segments 52 and 56
		DataStream<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>> cars_in_segments = mapStream
				.filter(new FilterFunction<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>>(){
					public boolean filter(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> in) throws Exception{
						return in.f6 <= 56 && in.f6 >= 52;
					}
				});

		//Voy a hacer pruebas solo com el VID 0, 1 y 2
		DataStream<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>> coches_prueba = cars_in_segments
				.filter(new FilterFunction<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>>(){
					public boolean filter(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> in) throws Exception{
						return in.f1 == 0 || in.f1 == 1 || in.f1 == 2;
					}
				});

		/*
		coches_prueba
				.keyBy(1)
				.reduce(new ReduceFunction<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>>() {
					public Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> reduce(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> t1, Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> t2) {
						return new Tuple8<>(t1.f0, t1.f1, t1.f2+t2.f2, t1.f3, t1.f4, t1.f5, t1.f6, t1.f7);
					}
				});

		*/

		//Assign a Timestamp based on the first column and key the tuples by the VID
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		KeyedStream<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>, Tuple> cars_in_segments_with_time = coches_prueba
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>>(){
					public long extractAscendingTimestamp(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> input){
						return input.f0*1000;
					}
				})
				.keyBy(1);



		WindowedStream<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>, Integer, TimeWindow> cars_windowed = cars_in_segments_with_time
				.window(EventTimeSessionWindows.withGap(Time.minutes(1)))
				.reduce(new ReduceFunction<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>>() {
					public Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> reduce(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> t1, Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> t2) {
						return new Tuple8<>(t1.f0, t1.f1, t1.f2+t2.f2, t1.f3, t1.f4, t1.f5, t1.f6, t1.f7);
					};

		/*
		SingleOutputStreamOperator<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>> sum_cars = cars_in_segments_with_time
				.reduce(new ReduceFunction<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>>(){
					public Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> sum_cars(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> t1, Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> t2) throws Exception{
						Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> out = new Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>(t1.f0, t1.f1, t1.f2+t2.f2, t1.f3, t1.f4, t1.f5, t1.f6, t1.f7);
						return out;
					}
				});


		SingleOutputStreamOperator<Tuple6<Long, Integer, Integer, Integer, Integer, Long>> f2_output = sum_cars
                .map(new MapFunction<Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long>, Tuple6<Long, Integer, Integer, Integer, Integer, Long>>() {
                    @Override
                    public Tuple6<Long, Integer, Integer, Integer, Integer, Long> map(Tuple8<Long, Integer, Long, Integer, Integer, Integer, Integer, Long> input) throws Exception { //Enters a tuple8 and exits a tuple6
                        Tuple6<Long, Integer, Integer, Integer, Integer, Long> output = new Tuple6(input.f0, input.f1, input.f3,
                                input.f6, input.f5, input.f2);
                        return output; //Returns the tuple6 for the output of the SpeedRadar functionality
                    }
                });
   				 */

		cars_windowed.writeAsCsv(output_path + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Flink Streaming Java API Skeleton");
	}
}