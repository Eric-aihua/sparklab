package com.eric.lab.spark.runner.outputformat;

import com.eric.lab.utils.JsonUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

// 演示spark对es的读写操作:elastic-hadoop的文档：https://github.com/elastic/elasticsearch-hadoop
public class ElasticReaderWriterRunner {

    public static final String ES_HOST = "mysqlserver";

    public static void main(String args[]) {
//        SparkConf conf = new SparkConf().setAppName("Read PB object sample");
        SparkConf conf = new SparkConf().setAppName("Read PB object sample").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Map<String, String> srcESConfig = new HashMap<String, String>();
        srcESConfig.put("es.nodes", ES_HOST);
        srcESConfig.put("es.resource", "ti_ip_repo_innovation_shodan_scanner_2018-12-17/data");
        srcESConfig.put("es.port", "9200");
        JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(sc, srcESConfig);

        Map<String, String> dstESConfig = new HashMap<String, String>();
        dstESConfig.put("es.nodes", ES_HOST);
        dstESConfig.put("es.resource", "spark_es_test/data");
        dstESConfig.put("es.port", "9200");

        //保留以112开头的IP
        JavaPairRDD<String, Map<String, Object>> filtedESRDD = esRDD.filter(new Function<Tuple2<String, Map<String, Object>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Map<String, Object>> v1) throws Exception {
                return v1._2.get("ip_str").toString().startsWith("112");
            }
        });


        readElasticRecord(esRDD);
        writeDataToES(filtedESRDD, dstESConfig);
    }

    //写数据到es
    private static void writeDataToES(JavaPairRDD<String, Map<String, Object>> filtedESRDD, Map<String, String> dstSourceESConfig) {
        //将要写
        JavaRDD<Map<String,Object>> esRecordRDD=filtedESRDD.map(new Function<Tuple2<String,Map<String,Object>>, Map<String,Object>>() {
            @Override
            public Map<String, Object> call(Tuple2<String, Map<String, Object>> v1) throws Exception {
                Map<String, Object> esRecord =new HashMap<>();
                esRecord.put("ip",v1._2.get("ip_str"));
                esRecord.put("isp", JsonUtils.parseJSON2Map(v1._2.get("origin_data").toString()).get("isp"));
                return esRecord;
            }
        });
        JavaEsSpark.saveToEs(esRecordRDD,dstSourceESConfig);

    }

    //读取记录中的IP信息
    private static void readElasticRecord(JavaPairRDD<String, Map<String, Object>> esRDD) {
        System.out.println(esRDD.partitions().size());
        esRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Map<String, Object>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Map<String, Object>>> tuple2Iterator) throws Exception {
                Tuple2<String, Map<String, Object>> record = tuple2Iterator.next();
                while (record != null && tuple2Iterator.hasNext()) {
                    System.out.println("Record Ip:" + record._2.get("ip_str"));
                    record = tuple2Iterator.next();
                }
            }
        });
    }


}
