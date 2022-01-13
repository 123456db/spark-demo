import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Function1;
import scala.runtime.BoxedUnit;

public class SimpleApp3 {
	public static void main(String[] args) {
		System.setProperty("HADOOP_USER_NAME", "root");

		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Java Spark Hive Example")
				.set("spark.testing.memory", "471859200").set("dfs.client.use.datanode.hostname", "true");

		SparkSession spark = SparkSession.builder().config(sparkConf)
				// .config("spark.sql.warehouse.dir","hdfs://****/user/hive/warehouse")
				.enableHiveSupport().getOrCreate();

		spark.sql("show databases").show();
		spark.sql("use default");
//		spark.sql("show tables").show();

		// spark.sql("select * from user_visit_action").show();
		// 统计品类的点击数量：（品类ID，点击数量）
		//click_category_id click_product_id
		spark.sql("create table user_visit_action02 (\r\n" + 
				"date	string\r\n" + 
				",user_id	bigint\r\n" + 
				",session_id	string\r\n" + 
				",page_id	bigint\r\n" + 
				",action_time	string\r\n" + 
		         ",search_key	string\r\n" + 
				",click_category_id	bigint\r\n" + 
				",click_product_id	bigint\r\n" + 
				",order_category_ids	string\r\n" + 
				",order_product_ids	string\r\n" + 
				",pay_category_ids	string\r\n" + 
				",pay_product_ids	string\r\n" + 
				",city_id	bigint\r\n" + 
				")\r\n" + 
				"row format delimited\r\n" + 
				"fields terminated by '_';");
//		spark.sql("select * from  user_visit_action where click_category_id !=-1 ").groupBy("click_category_id").count().show();
		spark.sql(" load data local inpath 'datas/user_visit_action.txt' into table user_visit_action02");
		spark.sql("select * from user_visit_action02 where city_id != 10").show();
		// spark.sql("select * from hive01").show();
		spark.close();

	}
}
