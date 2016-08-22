package summerschool;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

public class FlinkDataProcessing {
	
	public static void main(String[] args) throws Exception {
		
		// Now your turn… Batch data processing with Apache Flink. Find out which event type occurs most!
		
		// set up the execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//		Open FlinkDataProcessing.java
		//		Import the events.csv from text file 
		//		Include the fields type and provider in a Tuple2 dataset

		// id, type, timestamp_created, latitude, longitude, altitude, accuracy, bearing, provider, speed
		// shipment_number, sensor_id, resolution, power_consumption, battery_percentage, battery_temperature, 
		// acceleration, brightness, orientation_fw_bw, orientation_l_r, temperature, pressure, relative_humidity
		String eventPath = "/home/scads/logistics/datasets/imports/events.csv";
		DataSource<Tuple2<String, String>> events = env.readCsvFile(eventPath)
				  .fieldDelimiter(",")
				  .ignoreFirstLine()
				  .includeFields("0100000010000000000000")
				  .types(String.class, String.class);
		
		//events.print();
				
		//		group the data by type. Count the elements per group.
		AggregateOperator<Tuple2<String, Long>> group = events.map(new MapFunction<Tuple2<String,String>, Tuple2<String, Long>>() {

			public Tuple2<String, Long> map(
					Tuple2<String,String> arg0)
					throws Exception {
				return new Tuple2<String, Long>(arg0.f0, 1L);
			}
		}).groupBy(0).sum(1);
		group.print();
		
		//filter out provider = "\N" or "" (result 32302 elememts should be filtered)
		FilterOperator<Tuple2<String, String>> filteredData = events.filter(new FilterFunction<Tuple2<String, String>>() {
			
			public boolean filter(Tuple2<String, String> tuple)
					throws Exception {
				// TODO Auto-generated method stub
				return tuple.f1.equals("\\N") || tuple.f1.equals("\"\"");
			}
		});
		System.out.println("Count: " + filteredData.count());

		// execute program
		//env.execute("Flink Business Data");
	}
	
}