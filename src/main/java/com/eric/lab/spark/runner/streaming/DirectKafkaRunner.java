package com.eric.lab.spark.runner.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.Serializable;
import java.util.*;

/*使用Direct的方式从Kafka读取数据，并将结果写回kafka
* 参考文档：http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
*  * 操作流程
 * 1： 执行python update_state_for_kafka_faker.py.模拟启动测试数据的producer向kafka写数据
 * 2： 执行kafka_direct_streaming.sh.启动spark,并从spark获取数据并处理，最终结果写回kafka
 * 3:  进入logstash目录，执行：./logstash -f ../kafka-to-file.conf  从kafka收取结果，并将结果写入本地文件
* */
public class DirectKafkaRunner extends StatefulRunner implements Serializable {
    public static final String SPARK_STREAMING_RESULT_TOPIC = "spark_streaming_result";
    private static Collection<String> topics = Arrays.asList("spark_streaming_test");

    private static Map<String, Object> consumerKafkaParams = initConsumerKafkaParameters();
    private static Map<String, Object> produceKafkaParams = initProducerKafkaParameters();
    private static Producer<String, String> producer = new KafkaProducer<>(produceKafkaParams);

    public static void main(String args[]) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("SocketWordCount");
        StreamingRunnerUtils.optimizeConf(conf);
        JavaStreamingContext streamingContext = StreamingRunnerUtils.createStreamingContext(conf);


        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, consumerKafkaParams)
                );
        JavaDStream<String> recordRDD = stream.map(record -> record.value());
        DirectKafkaRunner kafkaRunner=new DirectKafkaRunner();
        kafkaRunner.processMaliciousRDD(recordRDD);
        streamingContext.start();
        // 等待作业完成
        streamingContext.awaitTermination();
    }

    /**
     * 将最终处理的结果发送到kafka
     * @param stringLongJavaPairRDD
     */
    @Override
    protected  void processResult(JavaPairRDD<String, Long> stringLongJavaPairRDD) {
        sendMsgToKafka(stringLongJavaPairRDD.collect().toString());
    }

    private  void sendMsgToKafka(String s) {
        producer.send(new ProducerRecord<String,String>(SPARK_STREAMING_RESULT_TOPIC,s));
        System.out.println("Send "+s+" to kafka");
    }

    private static  Map<String, Object> initConsumerKafkaParameters() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "malicious_process_streaming");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }
    private static  Map<String, Object> initProducerKafkaParameters() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("delivery.timeout.ms", 30000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

}
