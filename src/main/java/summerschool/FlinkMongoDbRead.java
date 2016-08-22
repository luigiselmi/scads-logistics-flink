package summerschool;

import java.io.Serializable;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.mapred.JobConf;
import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.mapred.MongoOutputFormat;

public class FlinkMongoDbRead {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
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
		
		List<Location> out = locations.map(new printLocation()).collect();
		
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
