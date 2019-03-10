package com.cz;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 *  广播变量
 * @date  2019/3/10 20:34
 * @author  by jaclon
 */
public class BroadcastVariable {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("BroadcastVariable").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //共享变量
        final int factor = 2;
        // 在java中创建共享变量，就是使用sparkContext的broadcast()方法
        // 返回的类型是Broadcast<T>
        // 使用时，就是调用其value()方法
        final Broadcast<Integer> broadcast = sparkContext.broadcast(factor);

        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        JavaRDD<Integer> parallelize = sparkContext.parallelize(integers);
        JavaRDD<Integer> newResult = parallelize.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                int factor= broadcast.value();
                return integer * factor;
            }
        });

        newResult.foreach(ls->{
            System.out.println(">>>> result:" + ls);
        });

        sparkContext.close();

    }
}
