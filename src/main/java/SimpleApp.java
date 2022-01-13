import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
	public static void main(String[] args) {
	    String logFile = "D:\\spark-3.2.0-bin-hadoop3.2\\README.md"; // Should be some file on your system
	    JavaSparkContext sc = new JavaSparkContext("local", "Simple App",
	      "D:\\spark-3.2.0-bin-hadoop3.2\\", new String[]{"target/spark-demo-0.0.1-SNAPSHOT.jar"});
	    JavaRDD<String> logData = sc.textFile(logFile).cache();

	    long numAs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("a"); }
	    }).count();

	    long numBs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("b"); }
	    }).count();
	    
	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	  
	}
}
