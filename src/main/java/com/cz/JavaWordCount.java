package com.cz;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class JavaWordCount {
    public static void main(String[] args) {
        //1.先创建conf对象进行配置，主要是设置名称，为了设置运行模式
        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
        //2.创建context对象
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("dir/file.txt");
        //3.进行切分数据 --flatMapFunction是具体实现类
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            //Iterable是所有集合的超级父接口
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> splited = Arrays.asList(s.split(" "));
                return splited.iterator();
            }
        });
        //4.将数据生成元组
        //第一个泛型是输入的数据类型，后两个参数是输出参数元组的数据
        final JavaPairRDD<String, Integer> tuples = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        //5.聚合
        JavaPairRDD<String, Integer> sumed = tuples.reduceByKey(new Function2<Integer, Integer, Integer>() {
            /**
             *
             * @param v1 相同key对应的value
             * @param v2 相同key对应的value
             * @return
             * @throws Exception
             */
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        //因为Java API 没有提供sortedBy 算子，此时需要将元组中的数据进行位置调换，排完序再换回来
        //第一次交换是为了排序
        JavaPairRDD<Integer, String> swaped = sumed.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tup) throws Exception {
                return tup.swap();
            }
        });
        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);

        //第二次交换是为了最终结果 <单词，数量>
        JavaPairRDD<String, Integer> res = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tup) throws Exception {
                return tup.swap();
            }
        });
        System.out.println(res.collect());
        res.saveAsTextFile("out4");
        jsc.stop();
    }
}
