package com.eric.lab.spark.runner.partitioner;

import org.apache.spark.Partitioner;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * 自定义域名分类器，host相同的域名将被分到同一个分区
 */
public class DomainPartitioner extends Partitioner {

    private int numPartition = 1;

    public DomainPartitioner(int numPartition) {
        this.numPartition = numPartition;
    }


    @Override
    public int numPartitions() {
        return numPartition;
    }

    @Override
    public int getPartition(Object key) {
        int code = 1;
        String domain = "";
        try {
            domain = new URL(key.toString()).getHost();

            code = domain.hashCode() % numPartition;
//            System.out.println(domain.hashCode()+":"+numPartition);

        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        if (code < 0) {
            return code + numPartition;
        }
//        System.out.println(key + ":"+numPartition+":"+":"+domain.hashCode());
        return code;
    }
//
//    public static void main(String args[]){
//        DomainPartitioner domainPartitioner = new DomainPartitioner(3);
//        System.out.println(domainPartitioner.getPartition("http://wwww.1baidu.com"));
//        System.out.println(domainPartitioner.getPartition("http://wwww.sogo.com"));
//        System.out.println(238328960%3);
//    }

}
