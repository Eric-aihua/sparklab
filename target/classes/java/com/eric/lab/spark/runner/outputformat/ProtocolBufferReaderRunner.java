package com.eric.lab.spark.runner.outputformat;

import com.eric.lab.spark.runner.outputformat.pojo.PersonEntity;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.List;

// 读写protobuf格式数据
public class ProtocolBufferReaderRunner {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("Read PB object sample");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 先生成若干个Person对象
        JavaRDD<PersonEntity.Person> readperson = sc.sequenceFile("file:///tmp/sparklab/pbtest", NullWritable.class, BytesWritable.class)
                .map(new Function<Tuple2<NullWritable, BytesWritable>, PersonEntity.Person>() {

                    public PersonEntity.Person call(Tuple2<NullWritable, BytesWritable> tuple) throws Exception {
                        // 解析byte[]为java对象，注意，一定要用copyBytes()而不是getBytes()
                        PersonEntity.Person p3 = PersonEntity.Person.parseFrom(tuple._2.copyBytes());
                        return p3;
                    }
                });
// 看一下结果
        List<PersonEntity.Person> list = readperson.collect();
        for (PersonEntity.Person person : list) {
            System.out.println(person.toString());
        }


    }
}
