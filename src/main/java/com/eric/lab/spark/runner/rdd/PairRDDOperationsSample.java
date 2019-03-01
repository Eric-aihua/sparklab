package com.eric.lab.spark.runner.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.eric.lab.spark.runner.rdd.RDDBaseOperationsSample.generateRangeList;

/**
 * 键值对类型的RDD操作
 */
public class PairRDDOperationsSample {
    public static void main(String args[]) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[2]").setAppName("RDD Sample"));
        JavaRDD<String> files = sc.parallelize(Arrays.asList("Spark powers a stack of libraries including SQL and DataFrames, MLlib for machine learning, GraphX, and Spark Streaming. You can combine these libraries seamlessly in the same application."
                , "Spark offers over 80 high-level operators that make it easy to build parallel apps. And you can use it interactively from the Scala, Python, R, and SQL shells.", ""));
        JavaPairRDD<String, Integer> wordsPair = convertRDDToPair(files);
        JavaPairRDD<Integer, Integer> intPair = sc.parallelizePairs(Arrays.asList(new Tuple2(1, 2), new Tuple2(1, 3), new Tuple2(2, 2), new Tuple2(3, 2), new Tuple2(5, 2)));
        JavaPairRDD<String, Integer> charPair = sc.parallelizePairs(Arrays.asList(new Tuple2("a", 2), new Tuple2("a", 3), new Tuple2("b", 2), new Tuple2("b", 4), new Tuple2("c", 2)));
        JavaPairRDD<String, Integer> charPair2 = sc.parallelizePairs(Arrays.asList(new Tuple2("a", 4),  new Tuple2("b", 3), new Tuple2("e", 2)));
//        processPairByReduceByKey(wordsPair);
//        processPairByGroupBykey(intPair);
//        processPairByMapValues(intPair);
//        processPairByFlatmapValues(intPair);
//        getAvgByMapValuesAndReduceByKey(charPair);
//        getAvgByCombinByKey(charPair);
        processPairByJoin(charPair,charPair2);



    }

    //对两个RDD进行join操作,结果如下
    //cogroup:[(b,([2, 4],[3])), (e,([],[2])), (a,([2, 3],[4])), (c,([2],[]))]
    //join:[(b,(2,3)), (b,(4,3)), (a,(2,4)), (a,(3,4))]
    //leftOuterJoin:[(b,(2,Optional[3])), (b,(4,Optional[3])), (a,(2,Optional[4])), (a,(3,Optional[4])), (c,(2,Optional.empty))]
    //rightOuterJoin:[(b,(Optional[2],3)), (b,(Optional[4],3)), (e,(Optional.empty,2)), (a,(Optional[2],4)), (a,(Optional[3],4))]
    //fullOuterJoin:[(b,(Optional[2],Optional[3])), (b,(Optional[4],Optional[3])), (e,(Optional.empty,Optional[2])), (a,(Optional[2],Optional[4])), (a,(Optional[3],Optional[4])), (c,(Optional[2],Optional.empty))]
    private static void processPairByJoin(JavaPairRDD<String, Integer> charPair, JavaPairRDD<String, Integer> charPair2) {
        System.out.println("cogroup:"+charPair.cogroup(charPair2).collect());
        System.out.println("join:"+charPair.join(charPair2).collect());
        System.out.println("leftOuterJoin:"+charPair.leftOuterJoin(charPair2).collect());
        System.out.println("rightOuterJoin:"+charPair.rightOuterJoin(charPair2).collect());
        System.out.println("fullOuterJoin:"+charPair.fullOuterJoin(charPair2).collect());
    }

    // 使用 combineByKey() 来计算每个键的对应值的均值
    private static void getAvgByCombinByKey(JavaPairRDD<String, Integer> charPair) {
        // 1,来创建每个键对应的累加器的初始值。这一过程会在每个分区中第一次出现各个键时发生，而不是在整个 RDD 中第一次出现一个键时发生.
        Function<Integer,AvgCount> initValue = new Function<Integer, AvgCount>() {
            @Override
            public AvgCount call(Integer v1) throws Exception {
                // 当在分区中第一次发现某个key时，记录对应的V以及将计数设为1
                return new AvgCount(v1,1);
            }
        };
        // 2.用来处理当前分区之前已经遇到的键，该方法将该键的累加器对应的当前值与这个新的值进行合并
        Function2<AvgCount,Integer,AvgCount>  mergePartitionValue = new Function2<AvgCount, Integer, AvgCount>() {
            @Override
            public AvgCount call(AvgCount v1, Integer v2) throws Exception {
                //用对应key的v加上当前的值，并计数加1
                return new AvgCount(v1.getTotal()+v2,v1.getNum()+1);
            }
        };
        // 3.用来合并某个Key在所有分区的结果，并最终计算出Key的最终结果
        Function2<AvgCount,AvgCount,AvgCount> combinAllPartition = new Function2<AvgCount, AvgCount, AvgCount>() {
            @Override
            public AvgCount call(AvgCount v1, AvgCount v2) throws Exception {
                return new AvgCount(v1.getTotal()+v2.getTotal(),v1.getNum()+v2.getNum());
            }
        };
        JavaPairRDD<String,Double> avgResult =charPair.combineByKey(initValue,mergePartitionValue,combinAllPartition).mapValues(new Function<AvgCount, Double>() {
            @Override
            public Double call(AvgCount v1) throws Exception {
//                System.out.println(v1);
                return v1.avg();
            }
        });
        System.out.println(avgResult.collect());
        //以Map的形式进行统计
        System.out.println(avgResult.collectAsMap());
    }

    //使用 reduceByKey() 和 mapValues() 来计算每个键的对应值的均值
    private static void getAvgByMapValuesAndReduceByKey(JavaPairRDD<String, Integer> charPair) {
        //1：将每个value转化成(v,1)的形式
        JavaPairRDD<String,Tuple2<Integer,Integer>> mapValuesResult =charPair.mapValues(s -> new Tuple2<Integer,Integer>(s,1));
        JavaPairRDD<String,Tuple2<Integer,Integer>> reduceByKeyResult= mapValuesResult.reduceByKey(new Function2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2 call(Tuple2<Integer,Integer> v1, Tuple2<Integer,Integer> v2) throws Exception {
                //2：算出每个value的值以及数量
                return new Tuple2(v1._1+v2._1,v1._2+v2._2);
            }
        });
        JavaRDD<Tuple2<String,Double>> avgResult = reduceByKeyResult.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Tuple2<String,Double>>() {
            @Override
            public Tuple2<String,Double> call(Tuple2<String, Tuple2<Integer, Integer>> v1) throws Exception {
                return new Tuple2<String,Double>(v1._1,v1._2._1/(double)v1._2._2);
            }
        });
        System.out.println(avgResult.collect());


    }

    // 和RDD的flatMap类似，输出结果：[(1,0), (1,1), (1,2), (1,0), (1,1), (1,2), (1,3), (2,0), (2,1), (2,2), (3,0), (3,1), (3,2), (5,0), (5,1), (5,2)]
    private static void processPairByFlatmapValues(JavaPairRDD<Integer, Integer> intPair) {
        System.out.println(intPair.flatMapValues(s -> range(s)).collect());
    }

    private static Iterable<Integer> range(int s) {
        List<java.lang.Integer> ints = new ArrayList<java.lang.Integer>();
        for (int i = 0; i <= s; i++) {
            ints.add(i);
        }
        return ints;
    }

    // 使用MapByvalues处理,输出：[(1,20), (1,30), (2,20), (3,20), (5,20)]
    private static void processPairByMapValues(JavaPairRDD<Integer, Integer> intPair) {
        //每个value乘以10
        System.out.print(intPair.mapValues(s -> s * 10).collect());
    }

    // 使用groupByKey处理
    private static void processPairByGroupBykey(JavaPairRDD<Integer, Integer> intPair) {
        System.out.print(intPair.groupByKey().collect());
    }

    // 使用reduceByKey处理Pair
    private static void processPairByReduceByKey(JavaPairRDD<String, Integer> wordsPair) {
        JavaPairRDD<String, Integer> reduceResult = wordsPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                //对相同Key的count进行累加
                return v1 + v2;
            }
        });
        System.out.println(reduceResult.collect());
    }

    // 讲RDD类型的数据转为PairRDD类型
    private static JavaPairRDD<String, Integer> convertRDDToPair(JavaRDD<String> files) {
        //转成word列表
        JavaRDD<String> wordPair = files.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        //将每个单词转为Tuple2对象
        return wordPair.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
    }
}
