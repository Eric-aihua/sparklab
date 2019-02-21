package com.eric.lab.spark.runner.streaming.func;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;

import java.util.List;

public class UpdateMaliciousSum implements Function2<List<Long>,Optional<Long>,Optional<Long>> {
    /**
     * @param newEvents:新的事件
     * @param currentStat：已有的统计数据
     * @return
     * @throws Exception
     */
    @Override
    public Optional<Long> call(List<Long> newEvents, Optional<Long> currentStat) throws Exception {
        long oldStat = currentStat.or(0L);
//        System.out.println(oldStat);
        return Optional.of(oldStat+newEvents.size());
    }
}
