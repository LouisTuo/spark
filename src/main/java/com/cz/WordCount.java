package com.cz;

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

public class WordCount {

    private static final String PATH = "C:\\Users\\jaclon\\Desktop\\强.txt";

    public static void main(String[] args) {

        // 1.创建SparkConf对象，
        SparkConf sparkConf = new SparkConf().setAppName("WordCountLocal28").setMaster("local");

        // 2.创建JavaSparkContext对象
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // 3.针对输入源，创建一个初始的RDD
        JavaRDD<String> lines = sparkContext.textFile(PATH);

        //4.对初始的RDD就是transformation操作
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        //5.
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }
        });

        //6.
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //7.
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeard " + wordCount._2 + " times");
            }
        });

        //8.
        sparkContext.close();
    }
}
