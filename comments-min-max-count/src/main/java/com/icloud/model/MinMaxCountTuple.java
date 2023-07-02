package com.icloud.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MinMaxCountTuple implements Writable {

    private Date min = new Date();
    private Date max = new Date();
    private long count = 0;

    private static final SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public Date getMin() {
        return min;
    }

    public void setMin(Date min) {
        this.min = min;
    }

    public Date getMax() {
        return max;
    }

    public void setMax(Date max) {
        this.max = max;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        // 쓰여진 순서대로 데이터 읽기
        this.min = new Date(in.readLong());
        this.max = new Date(in.readLong());
        this.count = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.min.getTime());
        out.writeLong(this.max.getTime());
        out.writeLong(this.count);
    }

    @Override
    public String toString() {
        return sdt.format(this.min) + "\t" + sdt.format(this.max) + "\t" + this.count;
    }
}
