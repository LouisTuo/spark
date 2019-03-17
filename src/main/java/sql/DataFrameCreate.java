package sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

/**
 * 使用Json文件创建DataFrame
 *
 * @author jaclon
 * @date 2019/3/17 17:51
 */
public class DataFrameCreate {

    public static void main(String[] args) {
        //String path = "D:\\IDEA\\spark\\src\\main\\java\\sql\\test.json";
        String path = "D:\\IDEA\\spark\\src\\main\\resources\\people.json";
        SparkSession sparkSession = SparkSession.builder().appName("Test").master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> ds = sparkSession.read().json(path);
        ds.show();

        ds.printSchema();

        ds.select("name").show();

        // Select everybody, but increment the age by 1
        ds.select(col("name"), col("age").plus(1)).show();

        // Select people older than 21
        ds.filter(col("age").gt(25)).show();

        //Count people by age
        System.out.println("----Count people by age---");
        ds.groupBy("age").count().show();

        // Register the DataFrame as a SQL temporary view
        ds.createOrReplaceTempView("people");

        //
        Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM people");
        sqlDF.show();

        //







        sparkSession.close();
    }
}
