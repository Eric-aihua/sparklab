package com.eric.lab.spark.runner.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.List;

// 读写sequencefile格式数据
public class CompressionReaderRunner {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("Read PB object sample");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration config =new Configuration();
//        JavaRDD<String> readperson = sc.newAPIHadoopFile("file:///tmp/sparklab/compresstest", TextInputFormat.class,String.class, Integer.class,config)
        JavaRDD<String> readperson = sc.textFile("file:///tmp/sparklab/compresstest");
// 看一下结
        List<String> list = readperson.collect();
        for (String obj : list) {
            System.out.println(obj);
        }


    }
}
