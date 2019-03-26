package com.eric.lab.spark.runner.rdd;

import java.io.Serializable;

public class AvgCount implements Serializable{
    private int total;
    private int num;
    public AvgCount(int total, int num) {
        this.num=num;
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
    public double avg(){
        return total/(double)num;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("AvgCount{");
        sb.append("total=").append(total);
        sb.append(", num=").append(num);
        sb.append('}');
        return sb.toString();
    }
}
