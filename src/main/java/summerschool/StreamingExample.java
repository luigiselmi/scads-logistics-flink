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

public class StreamingExample {
	
	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		env.enableCheckpointing(15000);
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		// get input data by connecting to the socket
		SingleOutputStreamOperator<TransportEvent> eventData =
				env.socketTextStream("localhost", 9999, '\n')
				   .flatMap(new splitMap());
		//eventData.addSink(new printData());

		// Streaming Part 
		// find maximum speed value for a shipment number within last 5 seconds
		// remind the case that speed variable can be null ("\n")
		SingleOutputStreamOperator<Tuple2<String, Double>> speedWindowAggregation =
				eventData.map(new MapFunction<TransportEvent, Tuple2<String, Double>>() {
							
							public Tuple2<String, Double> map(TransportEvent transportEvent) throws Exception {

								if(transportEvent.speed.contains("\\N")) {
									return new Tuple2<String, Double>(transportEvent.shipment_number, 0.0d);
								}
								else {
									return new Tuple2<String, Double>(transportEvent.shipment_number, Double.parseDouble(transportEvent.speed));
								}
							}
						 })
						 .keyBy(0)
						 .timeWindow(Time.seconds(5), Time.seconds(1))
						 .max(1);
		speedWindowAggregation.print();
		
		// CEP Part - package was thrown or dropped: create an application that detects an ORIENTATION_SENSOR_EVENT followed by an ACCELERATION_SENSOR_EVENT within 1 second
		//	(shipmentID has to be the same)
		// which grouping attribute is appropriate --> remember the difficulties of distributed data processing and stateful data elements
		// Question: try out what happens without using the keyBy function. why is it important to use it?
		// create a function that gives an drop alert on console window

		KeyedStream<TransportEvent, Tuple> keyedData = eventData.keyBy("shipment_number");
//		keyedData.map(new printData());
		
		Pattern<TransportEvent, ?> dropPattern = Pattern.<TransportEvent>begin("orientationEvent")
	  		    .where(new FilterFunction<TransportEvent>() {

					private static final long serialVersionUID = -3072773148164697089L;

					
					public boolean filter(TransportEvent event) throws Exception {

						return event.type.contains("ORIENTATION_SENSOR_EVENT");
					}
				})
	  		    .followedBy("accelerationEvent")
	  		    .where(new FilterFunction<TransportEvent>() {

					private static final long serialVersionUID = -1512623643099722060L;

					
					public boolean filter(TransportEvent event) throws Exception {

						return event.type.contains("ACCELERATION_SENSOR_EVENT");
					}
	  		    })
	  		    .within(Time.seconds(3));

		PatternStream<TransportEvent> FlowFirstPatternStream = CEP.pattern(keyedData, dropPattern);
		DataStream<String> warning = FlowFirstPatternStream.select(new dropAlert());
		warning.print();

		env.execute();
		}

		private static class dropAlert implements PatternSelectFunction<TransportEvent, String> {

			private static final long serialVersionUID = 5726945320464252488L;

			public String select(Map<String, TransportEvent> pattern) throws Exception {

				TransportEvent orientEvent = pattern.get("orientationEvent");
				TransportEvent accelEvent = pattern.get("accelerationEvent");

				if(orientEvent.shipment_number.equals(accelEvent.shipment_number)) {
					return "Drop alert in shipment number: " + orientEvent.shipment_number + "! Parcel could be damaged!";
				}
				else {
					return "False alarm! Events are in different shipments!";
				}
			}
		}		
	}