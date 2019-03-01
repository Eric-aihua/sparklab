package com.eric.lab.spark.runner.rdd;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

// 主要用来演示对RDD的操作,不用shell提交，直接在IDE中执行
public class RDDBaseOperationsSample {
    public static void main(String args[]){
        JavaSparkContext sc =new JavaSparkContext(new SparkConf().setMaster("local[2]").setAppName("RDD Sample"));
        JavaRDD<Integer> ints = sc.parallelize(Arrays.asList(1,2,3,4,5));
        //获取当前分区数
//        System.out.println(ints.partitions().size());
//        processByFlatMap(ints);
        processByFold(ints);
//        processByAggregate(ints);
//        processByDoubleRDD(ints);
//        processByPersist(ints);

    }

    private static void processByPersist(JavaRDD<Integer> ints) {
        ints.persist(StorageLevel.MEMORY_ONLY());
        System.out.println(ints.count());
        System.out.println(ints.collect());

    }

    // 通过JavaDoubleRDD实现平均值
    private static void processByDoubleRDD(JavaRDD<Integer> ints) {
        System.out.println(ints.mapToDouble(new DoubleFunction<Integer>() {
            @Override
            public double call(Integer value) throws Exception {
                return (double)value;
            }
        }).mean());
    }

    // 使用aggreate来计算平均值
    private static void processByAggregate(JavaRDD<Integer> ints) {
        AvgCount init=new AvgCount(0,0);
        // 统计每个分区的total以及count
        Function2<AvgCount,Integer,AvgCount> partitionOperation = new Function2<AvgCount, Integer, AvgCount>() {
            @Override
            public AvgCount call(AvgCount v1, Integer v2) throws Exception {
                int total = v1.getTotal()+v2;
                int count = v1.getNum()+1;
                return new AvgCount(total,count);
            }
        };
        // 合并每个分区的结果
        Function2<AvgCount,AvgCount,AvgCount> combinOperation = new Function2<AvgCount, AvgCount, AvgCount>() {
            @Override
            public AvgCount call(AvgCount v1, AvgCount v2) throws Exception {
                int total = v1.getTotal()+v2.getTotal();
                int count = v1.getNum()+v2.getNum();
                return new AvgCount(total,count);
            }
        };
        AvgCount finalResult =  ints.aggregate(init,partitionOperation,combinOperation);
        // 算出平均值
        System.out.println(finalResult.avg());
    }

    //使用fold来处理数字列表:与 reduce() 接收的函数签名相同的函数，再加上一个“初始值”来作为每个分区第一次调用时的结果
    private static void processByFold(JavaRDD<Integer> ints) {
        // 控制分区的数量
        JavaRDD<Integer> newInts = ints.repartition(5);
        //打印每个分区的数字
        newInts.foreachPartition(new VoidFunction<Iterator<Integer>>() {
            @Override
            public void call(Iterator<Integer> integerIterator) throws Exception {
                List<Integer> currentPartitionNums = IteratorUtils.toList(integerIterator);
                System.out.println("Partion elements:"+currentPartitionNums);
            }
        });
        int result=newInts.fold(100,new Function2<Integer,Integer,Integer>(){
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        //每个分区的zeroValue为100,最后结果为
        System.out.println(result);
    }

    // 使用FlatMap处理数字列表
    private static void processByFlatMap(JavaRDD<Integer> ints) {
        JavaRDD<Integer> result=ints.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public Iterator<Integer> call(Integer num) throws Exception {
                return generateRangeList(num);
            }
        });
        System.out.println(result.collect());
    }

    public static Iterator<Integer> generateRangeList(Integer num) {
        List<Integer> ints =new ArrayList<Integer>();
        for(int i =0;i <=num;i++){
            ints.add(i);
        }
        return ints.iterator();
    }
}
