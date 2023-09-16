package com.icloud.reducer;

import com.icloud.writable.CountAverageTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageReducer extends Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple> {

    private CountAverageTuple result = new CountAverageTuple();

    @Override
    protected void reduce(IntWritable key, Iterable<CountAverageTuple> values, Context context)
            throws IOException, InterruptedException {
        float sum = 0;
        float count = 0;

        for (CountAverageTuple value : values) {
            sum += value.getCount() * value.getAverage();
            count += value.getCount();
        }

        result.setCount(count);
        result.setAverage(sum / count);

        context.write(key, result);
    }
}
