package com.cz;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 *  累加变量
 * @author jaclon
 * @date 2019/3/10 21:02
 */
public class AccumulatorVariable {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("AccumulatorVariable").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        // 创建Accumulator变量
        // 调用sparkContext.accumulator()方法
        final Accumulator<Integer> sum = sparkContext.accumulator(0);
        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> parallelize = sparkContext.parallelize(integers);
        parallelize.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                //在函数内部，就可以对accumulator调用add()方法实现累加
                sum.add(integer);
            }
        });

        System.out.println(">>>> " + sum);

        sparkContext.close();
    }
}
