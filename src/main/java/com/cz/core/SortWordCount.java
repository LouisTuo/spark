package com.cz.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 *  排序的计数器
 * @author LouisTuo
 * @date 2019/3/10 21:54
 */
public class SortWordCount {

    private static final String PATH = "C:\\Users\\jaclon\\Desktop\\hello.txt";

    public static void main(String[] args) {
        //创建sparkConf和sparkContext
        SparkConf sparkConf = new SparkConf().setAppName("SortWordCount").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sparkContext.textFile(PATH);

        //
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }
        });

        JavaPairRDD<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer int1, Integer int2) throws Exception {
                return int1 + int2;
            }
        });

        wordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(">>> " + tuple2._1 + ":"+ tuple2._2.toString());
            }
        });
        //反转
        JavaPairRDD<Integer, String> wordCountPair = wordCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<Integer, String>(tuple2._2, tuple2._1);
            }
        });
        // 排序，直接调用sortByKey()方法
        JavaPairRDD<Integer, String> sortedWordCount = wordCountPair.sortByKey(false);
        sortedWordCount.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> tuple2) throws Exception {
                System.out.println(">>>> " + "{" + tuple2._1 + "," + tuple2._2 + "}");
            }
        });

        sparkContext.close();

    }

}
