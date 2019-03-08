package com.eric.lab.spark.runner.outputformat;

import com.eric.lab.spark.runner.outputformat.pojo.PersonEntity;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.List;

// 读写sequencefile格式数据
public class SequenceFileReaderRunner {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("Read PB object sample");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //
        JavaRDD<String> readperson = sc.sequenceFile("file:///tmp/sparklab/sequencetest", Text.class, IntWritable.class)
                .map(new Function<Tuple2<Text, IntWritable>, String>() {

                    public String call(Tuple2<Text, IntWritable> tuple) throws Exception {
                        // 解析byte[]为java对象，注意，一定要用copyBytes()而不是getBytes()
                        return tuple._1.toString()+"_"+tuple._2.get();
                    }
                });
// 看一下结果
        List<String> list = readperson.collect();
        for (String obj : list) {
            System.out.println(obj);
        }


    }
}
