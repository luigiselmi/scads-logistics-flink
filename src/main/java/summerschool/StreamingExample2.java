package summerschool;

import java.util.Map;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.cep.*;

import cep.TransportEvent;


import cep.splitMap;

import org.apache.flink.cep.pattern.AndFilterFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class StreamingExample2 {
	
	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(8);
		env.enableCheckpointing(15000);
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		// get input data by connecting to the socket
		SingleOutputStreamOperator<TransportEvent> eventData =
				env.socketTextStream("localhost", 9999, '\n')
				   .flatMap(new splitMap());
		//eventData.addSink(new printData);

		// Streaming & CEP Part 2 - Extend aggregation example for CEP usage
		// first compute the average values of  temperature for the last 10 seconds
		// tip --> filter all transport events that have Null-Values for temperature or brightness
		// 	   --> create a new Tuple that contains 7 variables (String shipment_number, Double brightness, Double temperature, Double avg_brightness, Double avg_temperature, Double count_brightness, Double count_temperature)
		//	   --> you can use POJOs or TupleX<>
		// use CEP to detect

		SingleOutputStreamOperator<Tuple6<String, Double, Double, Double, Double, Double>> shipmentAltitudeStream = eventData
				.filter(new FilterFunction<TransportEvent>() {
					
					public boolean filter(TransportEvent transportEvent) throws Exception {
						return !transportEvent.speed.contains("\\N") && !transportEvent.altitude.contains("\\N");
					}
				})
				.keyBy("shipment_number")
//				.keyBy(0)
				.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//						//.sum(1)
				.fold(new Tuple6<String, Double, Double, Double, Double, Double>("", 0.0d, 0.0d, 0.0d, 0.0d, 0.0d),new AvgFold());
		shipmentAltitudeStream.print();

		//CEP FLIGHT_EVENT = AVG(300) && Altitude > 500
		Pattern<Tuple6<String, Double, Double, Double, Double, Double>, ?> examplePattern =
				Pattern.<Tuple6<String, Double, Double, Double, Double, Double>>begin("speedEvent")
					   .where(new FilterFunction<Tuple6<String, Double, Double, Double, Double, Double>>(){

						   public boolean filter(Tuple6<String, Double, Double, Double, Double, Double> event) throws Exception {
							   return event.f5 > 30.0d;
						   }
					   })
				 	   .followedBy("altitudeEvent")
					   .where(new FilterFunction<Tuple6<String, Double, Double, Double, Double, Double>>() {
						   
						   public boolean filter(Tuple6<String, Double, Double, Double, Double, Double> event) throws Exception {
							   return event.f1 > 400.d;
						   }
					   })
				       .within(Time.seconds(20));

		PatternStream<Tuple6<String, Double, Double, Double, Double, Double>> flightPatternStream = CEP.pattern(shipmentAltitudeStream, examplePattern);
		DataStream<String> flightEventWarning = flightPatternStream.select(new flightWarning());
		flightEventWarning.print();

		env.execute();
		}
	
	private static class AvgFold implements FoldFunction<TransportEvent, Tuple6<String, Double, Double, Double, Double, Double>> {
		

		public Tuple6<String, Double, Double, Double, Double, Double> fold(Tuple6<String, Double, Double, Double, Double, Double> total,
																		   TransportEvent current) throws Exception {
			double count = total.f4 + 1.0d;
			double sum = total.f3 + Double.parseDouble(current.speed);
			double avg = sum / count;

			return new Tuple6<String, Double, Double, Double, Double, Double>
					(current.shipment_number, Double.parseDouble(current.altitude), Double.parseDouble(current.speed), sum, count, avg);
		}
	}

		private static class dropAlert implements PatternSelectFunction<TransportEvent, String> {

			private static final long serialVersionUID = 5726945320464252488L;

			public String select(Map<String, TransportEvent> pattern) throws Exception {

				TransportEvent orientEvent = pattern.get("orientationEvent");
				TransportEvent accelEvent = pattern.get("accelerationEvent");

				if(orientEvent.shipment_number.equals(accelEvent.shipment_number)) {
	//				System.out.println("Drop alert in shipment number: " + orientEvent.shipment_number + "! Parcel could be damaged!");
					return "Drop alert in shipment number: " + orientEvent.shipment_number + "! Parcel could be damaged!";
				}
				else {
	//				System.out.println("Drop alert in shipment number: " + orientEvent.shipment_number + "! Parcel could be damaged!");
					return "False alarm! Events are in different shipments!";
				}
			}
		}

		private static class flightWarning implements PatternSelectFunction<Tuple6<String, Double, Double, Double, Double, Double>, String> {

			public String select(Map<String, Tuple6<String, Double, Double, Double, Double, Double>> pattern) throws Exception {
				Tuple6<String, Double, Double, Double, Double, Double> speedEvent = pattern.get("speedEvent");
				Tuple6<String, Double, Double, Double, Double, Double> altitudeEvent = pattern.get("altitudeEvent");

				if(altitudeEvent.f0.equals(speedEvent.f0)) {
					//				System.out.println("Drop alert in shipment number: " + orientEvent.shipment_number + "! Parcel could be damaged!");
					return "Warning for shipment number: " + altitudeEvent.f0 + "! AVG_Speed > 30 AND AVG_Altitude > 400!";
				}
				else {
					//				System.out.println("Drop alert in shipment number: " + orientEvent.shipment_number + "! Parcel could be damaged!");
					return "False alarm! Events are in different shipments!";
				}
			}
		}

	}
