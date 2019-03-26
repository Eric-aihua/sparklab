package com.eric.lab.spark.runner.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.Durations;
import scala.Tuple2;

import java.util.Arrays;

/*
* 通过Socket来接收数据进行wordcount 统计操作
* */

public class SocketWordCountStreamingRunner {
    public static void main(String args[]) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("SocketWordCount");
        // 时间窗口为1秒
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf,Durations.seconds(10));
        // 监听本地7777端口
        JavaDStream<String> lines=streamingContext.socketTextStream("localhost",7777);
        //执行wordcount计算
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();
        // 启动流计算环境StreamingContext并等待它"完成"
        streamingContext.start();
        // 等待作业完成
        streamingContext.awaitTermination();
    }
}
