package com.eric.lab.spark.runner.rdd;

import java.io.Serializable;

public class CharAvgCount implements Serializable{
    private String key;
    private int total;
    private int num;
    public CharAvgCount(String key,int total, int num) {
        this.num=num;
        this.key = key;
        this.total=total;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public double avg(){
        return total/(double)num;
    }
}
