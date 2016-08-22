package cep;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class splitMap implements FlatMapFunction<String, TransportEvent> {

        private static final long serialVersionUID = -1383331169868527628L;

        public void flatMap(String line, Collector<TransportEvent> event)
        throws Exception {

            String[] arr = line.split(",");
            event.collect(new TransportEvent(arr[0].toString(), arr[1].toString(), arr[2].toString(), arr[3].toString(), arr[4].toString(),
                    arr[5].toString(), arr[6].toString(), arr[7].toString(), arr[8].toString(), arr[9].toString(),
                    arr[10].toString(), arr[11].toString(), arr[12].toString(), arr[13].toString(), arr[14].toString(),
                    arr[15].toString(), arr[16].toString(), arr[17].toString(), arr[18].toString(), arr[19].toString(),
                    arr[20].toString(), arr[21].toString(), arr[22].toString()));
        }

}
