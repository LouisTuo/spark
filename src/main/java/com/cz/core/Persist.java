package com.cz.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 * @Classname Persist
 * @Description RDD持久化
 * @Date 2019/3/10 20:04
 * @Created by Louis
 */
public class Persist {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Persist").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("E:\\Java\\itcastgoods\\传智播客毕业设计资料和代码\\论文\\答辩.txt")
                ;

        long time1 = System.currentTimeMillis();
        long count = stringJavaRDD.count();
        long time2 = System.currentTimeMillis();


        System.out.println("第一次>>>>>" + count + ";costTime:" + String.valueOf(time2 - time1));

        long time3 = System.currentTimeMillis();
        long count2 = stringJavaRDD.count();
        long time4 = System.currentTimeMillis();

        System.out.println("第二次>>>>>" + count2 + ";costTime:" + String.valueOf(time4 - time3));

        //广播变量
        Broadcast<Long> broadcast = sparkContext.broadcast(count);
        sparkContext.close();
    }
}
