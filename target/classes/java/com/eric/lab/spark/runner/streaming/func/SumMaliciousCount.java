package com.eric.lab.spark.runner.streaming.func;

public class SumMaliciousCount implements org.apache.spark.api.java.function.Function2<Long, Long, Long> {
    @Override
    public Long call(Long aLong, Long aLong2) throws Exception {
        return aLong+aLong2;
    }
}
