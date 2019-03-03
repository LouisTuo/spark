package com.cz;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * transformation操作
 */
public class TransformationOperation {

    public static void main(String[] args) {
        // map();
        //filterTest();
       // flatMapTest();
        //testReduceByKey();
        testJoin();
    }

    /**
     * map 算子案例
     */
    private static void map() {
        //创建javaSparkConf
        SparkConf sparkConf = new SparkConf().setAppName("map").setMaster("local");
        //创建JavaSparkContext
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        //构造集合
        List<String> strings = Arrays.asList("1", "2", "3", "4", "5");
        //并行化化集合，创建初始化RDD
        JavaRDD<String> stringRDD = javaSparkContext.parallelize(strings);

        //使用map算子，将集合中每个元素都乘以2
        //map算子，是对任何类型的RDD，都可以调用
        //在java中，map算子接受的参数是Function对象
        //创建Function对象，一定会让你设置第二个泛型参数，这个泛型参数，就是返回的新元素类型，
        //同时call方法的返回类型，也必须与第二个泛型类型同步
        //在call方法内部，就可以对原始RDD中的每一个元素进行各种处理和计算，并返回一个新的元素
        //所有新的元素就是组成一个新的RDD
        JavaRDD<String> map = stringRDD.map(new Function<String, String>() {

            @Override
            public String call(String str) throws Exception {
                return str + "a";
            }
        });

        //打印新的RDD
        map.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("----" + s + "--------");
            }
        });

        //关闭javaSparckContext
        javaSparkContext.close();
    }

    /**
     * filter算子案例
     */
    public static void filterTest() {
        //创建
        SparkConf sparkConf = new SparkConf().setAppName("spark filter").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //模拟集合
        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

        //并行化集合，创建初始RDD
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(integers);
        //对RDD进行算子操作，过滤其中的偶数
        //filter算子，传入的也是function，大致和map相似
        // 但是filter算子返回的是boolean值
        JavaRDD<Integer> evenNumberRDD = numberRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });

        //打印
        evenNumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("filter算子" + integer);
            }
        });

        //关闭
        sparkContext.close();
    }

    /**
     * flatMap测试
     */
    public static void flatMapTest(){
        //创建sparkConf
        SparkConf sparkConf = new SparkConf().setAppName("flatMap").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //
        List<String> strings = Arrays.asList("Hello world", "Hello Spark");
        JavaRDD<String> javaRDD = sparkContext.parallelize(strings);
        JavaRDD<String> stringJavaRDD = javaRDD.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String str) throws Exception {
                return Arrays.asList(str.split(" ")).iterator();
            }
        });

        stringJavaRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("---" + s);
            }
        });

        //
        sparkContext.close();
    }

    /**
     * groupByKey + reduceByKey测试
     */
    private static void testReduceByKey() {
        SparkConf sparkConf = new SparkConf().setAppName("reduceByKey").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //模拟集合
        List<Tuple2<String, Integer>> scores = Arrays.asList(
                new Tuple2<>("A", 12),
                new Tuple2<String, Integer>("B", 15),
                new Tuple2<String, Integer>("B", 16),
                new Tuple2<String, Integer>("C", 20),
                new Tuple2<>("C", 23)
        );
        //并行化集合，创建JavaPairRDD
        JavaPairRDD<String, Integer> javaPairRDD = sparkContext.parallelizePairs(scores);

        //针对javaPairRDD，执行groupBy算子操作，并对每个班级的成绩进行分组
        JavaPairRDD<String, Iterable<Integer>> pairRDD = javaPairRDD.groupByKey();

        pairRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringTuple2) throws Exception {
                System.out.println("序号:" + stringTuple2._1);
                Iterator<Integer> ite = stringTuple2._2.iterator();
                while (ite.hasNext()){
                    System.out.println(ite.next());
                }
            }
        });

        //统计每个班级的总分
        System.out.println("---------统计每个班级的总分------------");
        JavaPairRDD<String, Integer> classPairRDD = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer int1, Integer int2) throws Exception {
                return int1 + int2;
            }
        });

        classPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println("班级" + stringIntegerTuple2._1 +",分数" +stringIntegerTuple2._2);
            }
        });
        //根据key排序
        JavaPairRDD<String, Integer> sortedRDD = classPairRDD.sortByKey();
        System.out.println("--------排序后------");
        sortedRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println("班级" + tuple2._1 +",分数" +tuple2._2);
            }
        });

        sparkContext.close();

    }

    /**
     * join和cogroup的用法
     */
    private static void testJoin(){
        SparkConf sparkConf = new SparkConf().setAppName("join").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //模拟集合
        List<Tuple2<String, Integer>> scores = Arrays.asList(
                new Tuple2<>("A班级", 12),
                new Tuple2<String, Integer>("B班级", 16),
                new Tuple2<>("C班级", 23)
        );

        //模拟集合
        List<Tuple2<String, String>> names = Arrays.asList(
                new Tuple2<>("A班级", "张三"),
                new Tuple2<String, String>("B班级", "李四"),
                new Tuple2<String, String>("C班级", "二狗")
        );
        JavaPairRDD<String, Integer> scorePairRDD = sparkContext.parallelizePairs(scores);
        JavaPairRDD<String, String> namePairRDD = sparkContext.parallelizePairs(names);

        JavaPairRDD<String, Tuple2<Integer, String>> join = scorePairRDD.join(namePairRDD);
        join.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, String>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, String>> tuple2) throws Exception {
                Tuple2<Integer, String> integerTuple2 = tuple2._2;

                System.out.println("-----名字:" +tuple2._2._2 +"，班级："+tuple2._1+",成绩："+ tuple2._2._1);
            }
        });


        sparkContext.close();


    }

}

