package com.eric.lab.spark.runner.outputformat;

import com.eric.lab.spark.runner.outputformat.pojo.PersonEntity;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

// 读写protobuf格式数据
public class ProtocolBufferWriterRunner {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("Write PB object sample");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 先生成若干个Person对象
        JavaRDD<Integer> numbersRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<PersonEntity.Person> persons = numbersRDD.map(new Function<Integer, PersonEntity.Person>() {

            public PersonEntity.Person call(Integer x) throws Exception {
                // TODO Auto-generated method stub
                PersonEntity.Person.Builder builder = PersonEntity.Person.newBuilder();
                builder.setSn("SN-"+x.toString());
                builder.setName("Name:"+"name"+x.toString());
                PersonEntity.Person person = builder.build();
                return person;
            }
        });
        // 将JavaRDD<Person> 转换为JavaPairRDD<NullWritable, BytesWritable> 最后保存到HDFS
        persons.mapToPair(new PairFunction<PersonEntity.Person, NullWritable, BytesWritable>(){

            public Tuple2<NullWritable, BytesWritable> call(PersonEntity.Person person) throws Exception {
                // 这里new BytesWritable(person.toByteArray()) 是将java对象序列化为protobuf二进制数组
                return new Tuple2<NullWritable, BytesWritable>(NullWritable.get(), new BytesWritable(person.toByteArray()));
            }
        }).saveAsNewAPIHadoopFile("file:///tmp/sparklab/pbtest", NullWritable.class, BytesWritable.class, SequenceFileOutputFormat.class);

    }
}
