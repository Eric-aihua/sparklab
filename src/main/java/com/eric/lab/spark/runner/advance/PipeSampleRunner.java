package com.eric.lab.spark.runner.advance;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * 通过pipe功能，实现多语言的功能合作
 */
public class PipeSampleRunner {
    public static void main(String args[]){
        String pythonFilePath = "/root/sc/spark_cb/pipe_python.py";
        String pythonFileName = "pipe_python.py";
        SparkConf conf = new SparkConf().setAppName("accmulator sample").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //将python代码下发到executor
        sc.addFile(pythonFilePath);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,34,5,3,8,2,4,1,4,4,5));
        //调用pipe脚本进行数据处理，并返回结果
        JavaRDD<String> pipeResult =rdd.pipe(SparkFiles.get(pythonFileName));
        System.out.println(pipeResult.collect());

    }
}
