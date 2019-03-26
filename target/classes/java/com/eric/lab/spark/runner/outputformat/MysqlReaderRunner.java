package com.eric.lab.spark.runner.outputformat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

// 读Mysql数据
public class MysqlReaderRunner {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("Read PB object sample");
        JavaSparkContext sc = new JavaSparkContext(conf);
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            JavaRDD jdbcRDD =  JdbcRDD.create(sc, getConnectionFactory(), "select id,name_en from zerp_auth_menu where ?<=id and id <=?", 1, 100, 2, v1 -> v1.getInt(1)+"_"+v1.getString(2));
            System.out.println("分区大小："+jdbcRDD.partitions().size());
            System.out.println(jdbcRDD.collect());

        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static JdbcRDD.ConnectionFactory getConnectionFactory() throws SQLException {
        return new JdbcRDD.ConnectionFactory() {

            @Override
            public Connection getConnection() throws Exception {
                return DriverManager.getConnection("jdbc:mysql://mysqlserver/zerp?serverTimezone=UTC&useSSL=false","root","root");
            }
        };
    }
}
