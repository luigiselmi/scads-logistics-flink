package summerschool;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;

public class firstSteps {

	public static void main(String[] args) throws Exception {
		// first steps with Flink
		// import a file and do some processing
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//id, shipment_number, sender_id, recipient_id, route_id, weight_value, weight_unit, status, 
		//dangerous, monitored, lost_connection, last_event_received, pickup_time_period_start, arrival_time_period_start, arrival_delayed, timestamp_created
		DataSource<Tuple5<String, String, String, String, String>> importCSV = 
				env.readCsvFile("/home/scads/logistics/datasets/imports/orders.csv")
				.fieldDelimiter(",") 
				.ignoreFirstLine()
				.includeFields("0100111100000000")
				.types(String.class, String.class,String.class,String.class,String.class);
		
		importCSV.print();
		
		AggregateOperator<Tuple2<String, Long>> mapreduce = importCSV.map(new MapFunction<Tuple5<String,String,String,String,String>, Tuple2<String, Long>>() {

			public Tuple2<String, Long> map(Tuple5<String, String, String, String, String> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Long>(tuple.f4, 1L);
			}
		}).groupBy(0).sum(1);
		
		mapreduce.print();
	}

}
