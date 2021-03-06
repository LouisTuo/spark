package com.cz.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 使用HDFS创建RDD
 * 案例：统计文本文件个数
 */
public class HDFSRDD {

    private static final String FILE_PATH = "hdfs://spark1:9000/spark.txt";

    public static void main(String[] args) {
        //修改，appName，去除setMaster
        SparkConf sparkConf = new SparkConf().setAppName("HDFSFileRDD");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //修改：把textFile（）内的路径修改为HDFS文件
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile(FILE_PATH);

        //统计文本文件内的字数
        JavaRDD<Integer> map = stringJavaRDD.map(new Function<String, Integer>() {

            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });

        int sum = map.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        System.out.println("结果" + sum);

        javaSparkContext.close();


    }
}
