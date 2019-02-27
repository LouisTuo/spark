package com.cz;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 并行化集合创建RDD
 */
public class ParallelizeCollection {

    public static void main(String[] args) {
        //创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("ParallelizeCollection")
                .setMaster("local");
        //创建JavaSparkContext
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //调用Spark的parallelize方法，并行化集合的方式创建RDD
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers);
        //执行Reduce算子操作
        ///相当于先进行1+2，然后执行3+3=6
        int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer int1, Integer int2) {
                return int1 + int2 ;
            }
        });
        //输出
        System.out.println("计算的结果" + sum);
        //关闭JavaSparkContext
        sparkContext.close();


    }
}
