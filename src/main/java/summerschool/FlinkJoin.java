package summerschool;
import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import java.io.Serializable;
import java.util.List;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin;
import org.apache.flink.api.java.operators.JoinOperator.EquiJoin;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.hadoop.mapred.JobConf;

public class FlinkJoin {

	public static void main(String[] args) throws Exception {

ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		String uri = "mongodb://127.0.0.1:27017/LogisticsData.locations";
		
		String query = "{\"country\":\"GERMANY\"}";
		DataSet<Tuple2<BSONWritable, BSONWritable>> input = readFromMongo(env, uri, query);

		DataSet<Location> locations = input.map(new MapFunction<Tuple2<BSONWritable, BSONWritable>, Location>() {
			public Location map(Tuple2<BSONWritable, BSONWritable> bson) throws Exception {
				BSONWritable bvalue = bson.getField(1);
				Object value = bvalue.getDoc();
				BasicDBObject point = (BasicDBObject) value;

				String name = point.get("name").toString();
				String street = point.get("street").toString();
				String house_number = point.get("house_number").toString();
				String zip = point.get("zip").toString();
				String city = point.get("city").toString();
				String state = point.get("state").toString();
				String country = point.get("country").toString();
				String latitude = point.get("latitude").toString();
				String longitude = point.get("longitude").toString();

				return new Location(name, street, house_number, zip, city, 
						state, country, latitude, longitude);
			}
		});
		
		String eventPath = "/home/scads/logistics/datasets/imports/events.csv";
		DataSource<Tuple3<String, String, String>> events = env.readCsvFile(eventPath)
				  .fieldDelimiter(",")
				  .ignoreFirstLine()
				  .includeFields("0001100000100000000000")
				  .types(String.class, String.class, String.class);
		
		FilterOperator<Tuple3<String, String, String>> filterNulls = events.filter(new FilterFunction<Tuple3<String,String,String>>() {

			public boolean filter(Tuple3<String, String, String> tuple) throws Exception {
				// TODO Auto-generated method stub
				return !tuple.f0.equals("\\N");
			}
		});

				
		DataSet<Location> cutLocationGeo = locations.map(new cutCoordinates());
		
		MapOperator<Tuple3<String, String, String>, Tuple3<String, String, String>> cutEventsGeo = 
				filterNulls.map(new MapFunction<Tuple3<String,String,String>, Tuple3<String,String,String>>() {

			public Tuple3<String, String, String> map(Tuple3<String, String, String> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple3<String, String, String>(
						tuple.f0.substring(0,5), tuple.f1.substring(0,5), tuple.f2);
			}
		});

		cutEventsGeo.first(11).print();
		List<Location> out = cutLocationGeo.map(new printLocation()).collect();
		
		DataSet<Tuple2<Location, Tuple3<String, String, String>>> joinedDataset = cutLocationGeo.join(cutEventsGeo)
					.where("latitude", "longitude")
					.equalTo("f0", "f1");
		
		//you either have to write your own Map function for printing on console
		//or implement your own JoinFunction (sse below) that returns a TupleX object that allows to use print()!
//					.with(new customJoin());
		
		joinedDataset.print();
		
	}
	
	static public class customJoin implements JoinFunction<Location, Tuple3<String, String, String>, 
	Tuple4<String, String, String, String>> {

		private static final long serialVersionUID = 1L;

		public Tuple4<String, String, String, String> join(Location loc, Tuple3<String, String, String> eve)
				throws Exception {
			// TODO Auto-generated method stub
			return new Tuple4<String, String, String, String>(loc.latitude, eve.f0, eve.f1, loc.longitude);
		}
		
	}
	
	public static class cutCoordinates implements MapFunction<Location, Location> {

		public Location map(Location tuple) throws Exception {

			String newLat = tuple.latitude.substring(0,5);
			String newLon = tuple.longitude.substring(0,5);
			
			return new Location(tuple.name, tuple.street, tuple.house_number, tuple.zip, 
					tuple.city, tuple.state, tuple.country, newLat, newLon);
		}
	}
	
	    //reader config
	public static DataSet<Tuple2<BSONWritable, BSONWritable>> readFromMongo(ExecutionEnvironment env, String uri, String query) {
		JobConf conf = new JobConf();
		conf.set("mongo.input.uri", uri);
		conf.set("mongo.input.query", query);
		MongoInputFormat mongoInputFormat = new MongoInputFormat();
		return env.createHadoopInput(mongoInputFormat, BSONWritable.class, BSONWritable.class, conf);
	}
	
	    //writer config
	public static void writeToMongo(DataSet<Tuple2<BSONWritable, BSONWritable>> result, String uri) {
		JobConf conf = new JobConf();
		conf.set("mongo.output.uri", uri);
		MongoOutputFormat<BSONWritable, BSONWritable> mongoOutputFormat = new MongoOutputFormat<BSONWritable, BSONWritable>();
		result.output(new HadoopOutputFormat<BSONWritable, BSONWritable>(mongoOutputFormat, conf));
	}
	
	public static class printLocation implements MapFunction<Location, Location> {
	
		public Location map(Location val) throws Exception {
			// TODO Auto-generated method stub
			System.out.println( val.name + " " + 
								val.street+ " " +
								val.house_number+ " " +
								val.zip+ " " +
								val.city+ " " +
								val.state+ " " +
								val.country+ " " +
								val.latitude+ " " +
								val.longitude
								);
			return val;
		}		
	}
	
	public static class Location implements Serializable {
		
		public String name, street, house_number, zip, city, state, country, latitude, longitude;

		public Location(){
			
		}
		
		public Location(String name, String street, String house_number, String zip, String city, 
				String state, String country, String latitude, String longitude) {
			
			this.name = name;
			this.street = street;
			this.house_number = house_number;
			this.zip = zip;
			this.city = city;
			this.state = state;
			this.country = country;
			this.latitude = latitude;
			this.longitude = longitude;
		}
	}
}
