package com.eric.lab.spark.runner.advance;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * 演示accmulator的使用
 */
public class AccmulatorSampleRunner {
    public static void main(String args[]){
        SparkConf conf = new SparkConf().setAppName("accmulator sample").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,34,5,3,8,2,4,1,4,4,5));
//        addOneTime(sc, rdd);
        addMoreTime(sc, rdd);

    }
    //把add操作放在map中，累加器会统计多次
    private static void addMoreTime(JavaSparkContext sc, JavaRDD<Integer> rdd) {
        Accumulator oodAccu =sc.accumulator(0);
        Accumulator evenAccu =sc.accumulator(0);
        JavaRDD<Integer> newRDD=rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
//                System.out.println("map 输出："+v1);
                if (v1%2==0){
                    evenAccu.add(1);
                }else{
                    oodAccu.add(1);
                }
                return v1*2;
            }
        });
        //在调用count以及collect的时候，都会触发map的执行，最终都导致累加器值的重复计算
        newRDD.count();
        newRDD.collect();
        System.out.println(evenAccu.value());
        System.out.println(oodAccu.value());

    }


    //把add操作放在foreach中，使累加器只加一次
    private static void addOneTime(JavaSparkContext sc, JavaRDD<Integer> rdd) {
        Accumulator oodAccu =sc.accumulator(0);
        Accumulator evenAccu =sc.accumulator(0);
        rdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                if (integer%2==0){
                    evenAccu.add(1);
                }else{
                    oodAccu.add(1);
                }
                // 在executor中读取计数器的值，会产生：Can't read accumulator value in task 异常
//                System.out.println(evenAccu.value());
            }
        });
        //只能再driver里读取计数器的值
        System.out.println(evenAccu.value());
        System.out.println(oodAccu.value());
    }
}
