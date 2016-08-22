package cep;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import cep.TransportEvent;

/**
 * Created by Norman Spangenberg on 27.04.2016.
 */
public class printData implements SinkFunction<TransportEvent>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7842299654691359608L;

	public void invoke(TransportEvent transportEvent) throws Exception {
		// TODO Auto-generated method stub
        System.out.println(transportEvent.shipment_number + ": " + transportEvent.altitude);

	}
}
