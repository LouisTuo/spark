package com.cz.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 统计文本每行文字出现的字数
 */
public class LineCount {

    public static void main(String[] args) {
        //创建SparkContxt
        SparkConf sparkConf = new SparkConf().setAppName("LineCount").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //创建初始的RDD，lines
        JavaRDD<String> lines = sparkContext.textFile("C:\\Users\\jaclon\\Desktop\\hello.txt");

        //对line RDD执行mapToPair算子，将每一行映射为(line,1)这种key_value的模式
        //然后后面才能统计每一行出现的次数
        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,10);
            }
        });

        //对pair RDD执行reduceByKey算子，统计每一行出现的总字数
        JavaPairRDD<String, Integer> lineCounts = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer int1, Integer int2) throws Exception {
                return int1 + int2;
            }
        });

        //执行action操作，foreach，打印每一行出现的总字数
        lineCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " 出现 "+ t._2 + "次数");
            }
        });


        //关闭
        sparkContext.close();
    }
}
