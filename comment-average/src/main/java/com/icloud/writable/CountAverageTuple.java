package com.icloud.writable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountAverageTuple implements WritableComparable<CountAverageTuple> {

    private float count;
    private float average;

    @Override
    public void readFields(DataInput in) throws IOException {
        this.count = in.readFloat();
        this.average = in.readFloat();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(this.count);
        out.writeFloat(this.average);
    }

    @Override
    public int compareTo(CountAverageTuple other) {
        if (this.count < other.count) {
            return -1;
        } else if (this.count > other.count) {
            return 1;
        } else {
            return Float.compare(this.average, other.average);
        }
    }

    public float getCount() {
        return count;
    }

    public void setCount(float count) {
        this.count = count;
    }

    public float getAverage() {
        return average;
    }

    public void setAverage(float average) {
        this.average = average;
    }

    @Override
    public String toString() {
        return this.count + "\t" + this.average;
    }
}
