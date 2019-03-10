package com.cz;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class ActionOperation {

    public static void main(String[] args) {
        // reduce();
        // collect();
        countByKey();
    }

    /**
     * 测试reduce
     */
    private static void reduce() {
        // 创建sparkConf和javaSparkContext
        SparkConf sparkConf = new SparkConf().setAppName("TestReduce").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //有一个集合，对里面的10个数字进行累加
        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> integerJavaRDD = sparkContext.parallelize(integers);
        // reduce操作原理：首先将第一个和第二个元素传入call()方法进行计算，会获取一个结果，比如：1+2= 3
        // 接着将该结果和下一个元素传入call()方法，进行计算3+3
        // 以此类推...
        //  所以reduce的本质，就是聚合，将多个元素聚合成一个元素
        Integer sum = integerJavaRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer int1, Integer int2) throws Exception {
                return int1 + int2;
            }
        });
        System.out.println("计算的总和为：" + sum);
        //关闭
        sparkContext.close();
    }

    /**
     * collect,take,count用例
     */
    private static void collect() {
        SparkConf sparkConf = new SparkConf().setAppName("TestCollect").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        //将每个数据 X2
        JavaRDD<Integer> integersRDD = sparkContext.parallelize(integers);
        JavaRDD<Integer> doubleNumbers = integersRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        // 不用foreach遍历，在远程集群中遍历rdd中的元素
        // 使用collect操作，将分布在在远程集群中的doubleNumbers RDD数据拉取到本地
        // collect方式不建议使用，因为rdd数据量很大的情况，性能较差
        // 因为会从网络传输数据到本地，而且RDD数据量大会发生内存溢出，
        // 因此推荐使用foreach action操作来最终的RDD进行处理
        List<Integer> collect = doubleNumbers.collect();
        collect.forEach(ls -> {
            System.out.println("double数据为：" + ls);
        });

        System.out.println("---------------------");
        long count = doubleNumbers.count();
        System.out.println("count-------" + count);

        // take operation,和collect类似也是从远程集群中，获取RDD,
        // 但是collect是取所有数据，take是取前几个数据
        List<Integer> take = doubleNumbers.take(3);
        take.forEach(t -> {
            System.out.println("take-----" + t);
        });

        sparkContext.close();
    }

    /**
     * 测试countByKey
     */
    public static void countByKey() {

        SparkConf sparkConf = new SparkConf().setAppName("Test CountByName").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //构造集合
        List<Tuple2<String, String>> list = Arrays.asList(
                new Tuple2<>("湖北", "武汉"),
                new Tuple2<>("湖北", "随州"),
                new Tuple2<>("湖北", "襄阳"),
                new Tuple2<>("青海","西宁")
        );
        //并行化集合，创建JavaPairRDD
        // JavaRDD<Tuple2<String, String>> parallelize = sparkContext.parallelize(list);
        JavaPairRDD<String, String> pairRDD = sparkContext.parallelizePairs(list);
        //对JavaRDD应用
        Map<String, Long> map = pairRDD.countByKey();
        map.forEach(new BiConsumer<String, Long>() {
            @Override
            public void accept(String s, Long aLong) {
                System.out.println(s + "--" + aLong);
            }
        });
        sparkContext.close();
    }
}
