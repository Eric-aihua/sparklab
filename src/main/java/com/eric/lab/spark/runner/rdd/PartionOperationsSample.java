package com.eric.lab.spark.runner.rdd;

import com.eric.lab.spark.runner.partitioner.DomainPartitioner;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class PartionOperationsSample {
    public static void main(String args[]) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Partition Info Test").setMaster("local[2]"));
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
        JavaPairRDD<Integer, Integer> rdd2 = sc.parallelizePairs(Arrays.asList(new Tuple2(1, 4), new Tuple2(2, 3), new Tuple2(3, 2)));
        JavaPairRDD<String, Integer> urlRdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2("http://wwww.baidu.com/123/456", 4),
                new Tuple2("http://wwww.baidu.com/122222", 3),
                new Tuple2("http://wwww.google.com/123/456", 2),
                new Tuple2("http://wwww.360.com/122223/456", 2),
                new Tuple2("http://wwww.baidu.com/123/456", 3)));
//        getPartitionerInfo(rdd,rdd2);
        processByCustomPartitioner(urlRdd);


    }

    //使用自定义的分区器进行分区
    private static void processByCustomPartitioner(JavaPairRDD<String, Integer> urlRdd) {
        System.out.println(urlRdd.partitioner());
        printPartitionElement(urlRdd);
        //重新设置分区器
        JavaPairRDD<String, Integer> newPartRDD=urlRdd.partitionBy(new DomainPartitioner(3));
        System.out.println(newPartRDD.partitioner());
        printPartitionElement(newPartRDD);
    }

    private static void printPartitionElement(JavaPairRDD<String, Integer> urlRdd) {
        urlRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                for(Object tuple2:IteratorUtils.toList(tuple2Iterator)){
                    System.out.print(tuple2);
                }
            }
        });
    }

    //获取并设置新的partitioner
    private static void getPartitionerInfo(JavaRDD<Integer> rdd1, JavaPairRDD<Integer, Integer> rdd2) {
        System.out.println(rdd1.partitioner());
        JavaPairRDD<Integer, Integer> pairRDD = getIntegerIntegerJavaPairRDD(rdd1);
        System.out.println(pairRDD.partitioner());
        //设置HashPartitioner,如果不调用 persist() 的话，后续的 RDD 操作会对partitioned 的整个谱系重新求值，这会导致对 pairs 一遍又一遍地进行哈希分区操作
        JavaPairRDD<Integer, Integer> hashPairRdd = pairRDD.partitionBy(new HashPartitioner(3)).persist(StorageLevel.MEMORY_ONLY());
        System.out.println(hashPairRdd.partitioner());
        JavaRDD<String> mapResult = hashPairRdd.map(new Function<Tuple2<Integer, Integer>, String>() {
            @Override
            public String call(Tuple2<Integer, Integer> v1) throws Exception {
                return v1.toString();
            }
        });
        //map()的函数理论上可以改变元素的键，因此结果就不会有固定的分区方式
        System.out.println(mapResult.partitioner());

    }

    private static JavaPairRDD<Integer, Integer> getIntegerIntegerJavaPairRDD(JavaRDD<Integer> rdd) {
        return rdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<>(integer, 1);
            }
        });
    }
}
