package com.icloud.writable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MedianStdDevTuple implements WritableComparable<MedianStdDevTuple> {

    private float median;
    private float stdDev;

    @Override
    public void readFields(DataInput in) throws IOException {
        this.median = in.readFloat();
        this.stdDev = in.readFloat();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(this.median);
        out.writeFloat(this.stdDev);
    }

    public float getMedian() {
        return median;
    }

    public void setMedian(float median) {
        this.median = median;
    }

    public float getStdDev() {
        return stdDev;
    }

    public void setStdDev(float stdDev) {
        this.stdDev = stdDev;
    }

    @Override
    public String toString() {
        return this.median + "\t" + this.stdDev;
    }

    @Override
    public int compareTo(MedianStdDevTuple other) {
        int medianComp = Float.compare(this.median, other.getMedian());
        if (medianComp != 0) return medianComp;
        return Float.compare(this.stdDev, other.getStdDev());
    }
}
