import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Function1;
import scala.runtime.BoxedUnit;

public class SimpleApp2 {
	public static void main(String[] args) {
		System.setProperty("HADOOP_USER_NAME", "root");

		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Java Spark Hive Example")
				.set("spark.testing.memory", "471859200").set("dfs.client.use.datanode.hostname", "true");

		SparkSession spark = SparkSession.builder().config(sparkConf)
				// .config("spark.sql.warehouse.dir","hdfs://****/user/hive/warehouse")
				.enableHiveSupport().getOrCreate();

		spark.sql("show databases").show();
		spark.sql("use default").show();
//		spark.sql("show tables").show();

		// spark.sql("select * from user_visit_action").show();
		// 统计品类的点击数量：（品类ID，点击数量）
		//click_category_id click_product_id
		spark.sql("select click_category_id,a.* from  user_visit_action  a").show(false);
		spark.sql("select click_category_id,a.* from  user_visit_action  a").where("click_category_id > -1").show();//
//		spark.sql("select * from  user_visit_action where click_category_id!=-1").groupBy("click_category_id").count().show();

		// spark.sql("select * from hive01").show();
		spark.close();

	}
}
