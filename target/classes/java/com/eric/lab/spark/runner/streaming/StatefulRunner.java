package com.eric.lab.spark.runner.streaming;

/*
全局统计：每个IP可能包含多个攻击类型，统计流数据中各种攻击类型的数量
* */

import com.eric.lab.spark.runner.streaming.func.SumMaliciousCount;
import com.eric.lab.spark.runner.streaming.func.UpdateMaliciousSum;
import com.eric.lab.utils.JsonUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.eric.lab.spark.runner.streaming.StreamingRunnerUtils.BATCH_INVERVAL;

/**
 * Spark 有状态操作，包含window以及updateStateByKey的操作.
 * 操作流程
 * 1： 执行python update_state_for_tcp_faker.py.模拟启动server端
 * 2： 执行update_state_by_key.sh.启动spark,并从tcp端口获取数据并处理，最终结果将打印在控制台
 */
public class StatefulRunner implements Serializable {



    public static void main(String args[]) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("SocketWordCount");
        StreamingRunnerUtils.optimizeConf(conf);
        JavaStreamingContext streamingContext = StreamingRunnerUtils.createStreamingContext(conf);

        // 监听本地7777端口
        JavaDStream<String> lines = streamingContext.socketTextStream("localhost", 7777);
        StatefulRunner statefulRunner=new StatefulRunner();
        statefulRunner.processMaliciousRDD(lines);
        streamingContext.start();
        // 等待作业完成
        streamingContext.awaitTermination();
    }

    protected  void processMaliciousRDD(JavaDStream<String> lines) {
        JavaDStream<String> words = lines.flatMap(x -> genMaliciousList(x).iterator());
        JavaPairDStream<String, Long> pairs = words.mapToPair(s -> new Tuple2<>(s, 1l));
        pairs.persist();

        //统计一个window的记录条数
        JavaDStream<Long> windowCounts = pairs.countByWindow(Durations.seconds(BATCH_INVERVAL), Durations.seconds(BATCH_INVERVAL));
        // 打印结果
        windowCounts.foreachRDD(new VoidFunction<JavaRDD<Long>>() {
            @Override
            public void call(JavaRDD<Long> longJavaRDD) throws Exception {
                System.out.println("单window统计结果：" + longJavaRDD.collect());
            }
        });

        //统计一个window各个类型的数量
        JavaPairDStream<String, Long> windowWordCounts = pairs.reduceByKeyAndWindow(new SumMaliciousCount(), Durations.seconds(BATCH_INVERVAL), Durations.seconds(BATCH_INVERVAL));
        // 打印结果
        windowWordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> stringLongJavaPairRDD) throws Exception {
                System.out.println("单window统计结果：" + stringLongJavaPairRDD.collect());

            }
        });


        // 通过updateStateByKey持续统计各个恶意类型所有时间的数量
        JavaPairDStream<String, Long> wordCounts = pairs.updateStateByKey(new UpdateMaliciousSum());
        wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
                                  @Override
                                  public void call(JavaPairRDD<String, Long> stringLongJavaPairRDD) throws Exception {
                                      processResult(stringLongJavaPairRDD);
                                  }
                              }
        );
    }

    protected  void processResult(JavaPairRDD<String, Long> stringLongJavaPairRDD) {
        System.out.println("历史统计结果：" + stringLongJavaPairRDD.collect());
    }


    // 提取恶意信息
    private  List<String> genMaliciousList(String record) {
        String info = record.split("\t")[1];
        List<String> maliciousList = new ArrayList<String>();
        if (StringUtils.isNotEmpty(info)) {
            Map<String, Object> maliciousInfo = (Map<String, Object>) JsonUtils.parseJSON2Map(info).get("malicious_type");
            for (String key : maliciousInfo.keySet()) {
                if (key.startsWith("is_") && maliciousInfo.get(key).equals("true")) {
                    maliciousList.add(key);
                }
            }
        }

        return maliciousList;
    }
}
