package com.eric.lab.spark.runner.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingRunnerUtils {

    public static final int BATCH_INVERVAL =1;
    public static final String SPARK_CHECKPOINT = "file:///tmp/spark/checkpoint";
    protected static void optimizeConf(SparkConf conf) {
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kyro.registrationRequired", "true");
    }

    // 创建带有容错功能的Context
    protected static JavaStreamingContext createHAStreamingContext(SparkConf conf) {
        return JavaStreamingContext.getOrCreate(SPARK_CHECKPOINT, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                JavaSparkContext sparkContext=new JavaSparkContext(conf);
                JavaStreamingContext streamingContext=new JavaStreamingContext(sparkContext, Durations.seconds(BATCH_INVERVAL));
                streamingContext.checkpoint(SPARK_CHECKPOINT);
                return streamingContext;
            }
        });
    }

    // 使用常规的方式创建context
    protected static JavaStreamingContext createStreamingContext(SparkConf conf) {
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(BATCH_INVERVAL));
        streamingContext.checkpoint(SPARK_CHECKPOINT);
        streamingContext.sparkContext().setLogLevel("WARN");
        return streamingContext;
    }
}
