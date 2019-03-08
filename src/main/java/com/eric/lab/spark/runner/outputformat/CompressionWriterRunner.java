package com.eric.lab.spark.runner.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

// 写SequenceFile
public class CompressionWriterRunner {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("Write compression sample");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 先生成若干个Person对象
        JavaPairRDD<String, Integer> urlRdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2("http://wwww.baidu.com/123/456", 4),
                new Tuple2("http://wwww.baidu.com/122222", 3),
                new Tuple2("http://wwww.google.com/123/456", 2),
                new Tuple2("http://wwww.360.com/122223/456", 2),
                new Tuple2("http://wwww.baidu.com/123/456", 3)));

        //
        JavaPairRDD<String, Integer>writableRDD = urlRdd.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<String, Integer>(new String(stringIntegerTuple2._1),new Integer(stringIntegerTuple2._2));
            }
        });
        Configuration hadoopConf =new Configuration();
        //指定压缩算法
        hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true");
        hadoopConf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
        writableRDD.saveAsNewAPIHadoopFile("file:///tmp/sparklab/compresstest", String.class, Integer.class, TextOutputFormat.class,hadoopConf);

    }
}
