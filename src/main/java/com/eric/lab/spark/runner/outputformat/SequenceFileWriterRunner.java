package com.eric.lab.spark.runner.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

// 写SequenceFile
public class SequenceFileWriterRunner {
    public static void main(String args[]) {
        IntWritable intWritable =new IntWritable();
        SparkConf conf = new SparkConf().setAppName("Write sequence file sample");
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
        JavaPairRDD<Text, IntWritable>writableRDD = urlRdd.mapToPair(new PairFunction<Tuple2<String, Integer>, Text, IntWritable>() {
            @Override
            public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<Text, IntWritable>(new Text(stringIntegerTuple2._1),new IntWritable(stringIntegerTuple2._2));
            }
        });
        Configuration hadoopConf =new Configuration();
        writableRDD.saveAsNewAPIHadoopFile("file:///tmp/sparklab/sequencetest", Text.class, IntWritable.class, SequenceFileOutputFormat.class);
//        writableRDD.saveAsHadoopFile("file:///tmp/sparklab/sequencetest", Text.class, IntWritable.class, SequenceFileOutputFormat.class, GzipCodec.class);

    }
}
