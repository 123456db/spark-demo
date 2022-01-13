import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import net.minidev.json.JSONObject;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.runtime.BoxedUnit;

public class SimpleApp4 {
	public static void main(String[] args) {
		System.setProperty("HADOOP_USER_NAME", "root");

		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Java Spark Hive Example")
				.set("spark.testing.memory", "471859200").set("dfs.client.use.datanode.hostname", "true");

		SparkSession spark = SparkSession.builder().config(sparkConf)
//				 .config("spark.sql.warehouse.dir","hdfs://****/user/hive/warehouse")
				.enableHiveSupport().getOrCreate();

//		spark.sql("show databases").show();
//		spark.sql("use default");
//		spark.sql("show tables").show();

		//click_category_id	Long	某一个商品品类的 ID
//		Dataset<Row> click_category_ids = spark.sql("select * from  user_visit_action02 where click_category_id != -1 ").groupBy("click_category_id").count();

//		spark.sql("select * from  user_visit_action02 where order_category_ids is not null ").groupBy("order_category_ids").count().show(100000);;
//		Dataset<Row> order_category_ids = spark.sql("select * from  user_visit_action02 where order_category_ids != null ").groupBy("order_category_ids").count();
//		spark.sql("select * from  user_visit_action02 where search_key ='手机' ").show();
		
//		spark.sql("select * from  user_visit_action02 where search_key ='手机' and order_category_ids !='null' ").show();
//		spark.sql("select * from  user_visit_action02 where search_key ='手机' and order_category_ids is not null ").show();

		
//		spark.sql("select * from  user_visit_action02 where search_key ='手机' ").show(10);
		
		 
		
		
//		spark.sql("select order_category_ids from  user_visit_action02 ").groupBy("order_category_ids").count().show(false);
//		spark.sql("select * from  user_visit_action02").groupBy("order_category_ids").count().show(100000);
		spark.sql("select * from  default.user_visit_action02").show();

		spark.close();

	}
}
