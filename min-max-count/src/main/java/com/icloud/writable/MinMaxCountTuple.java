package com.icloud.writable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MinMaxCountTuple implements WritableComparable<MinMaxCountTuple> {

    private Date min = new Date();
    private Date max = new Date();

    private final static SimpleDateFormat sdf =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private long count = 0;

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
    public int compareTo(MinMaxCountTuple other) {
        int cmp = this.min.compareTo(other.min);
        if (cmp != 0) {
            return cmp;
        }

        return this.max.compareTo(other.max);
    }

    @Override
    public String toString() {
        return sdf.format(this.min) + "\t" + sdf.format(this.max) + "\t" + this.count;
    }
}
