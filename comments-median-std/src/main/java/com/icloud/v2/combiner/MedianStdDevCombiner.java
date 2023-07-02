package com.icloud.v2.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

public class MedianStdDevCombiner
        extends Reducer<IntWritable, SortedMapWritable<IntWritable>, IntWritable, SortedMapWritable<IntWritable>> {

    @Override
    protected void reduce(IntWritable key,
                          Iterable<SortedMapWritable<IntWritable>> values,
                          Context context)
            throws IOException, InterruptedException {
        SortedMapWritable<IntWritable> outValue = new SortedMapWritable<>();
        for (final Map<IntWritable, Writable> value : values) {
            for (Map.Entry<IntWritable, Writable> entry : value.entrySet()) {
                LongWritable count = (LongWritable) outValue.get(entry.getKey());
                if (count != null) {
                    count.set(count.get() + ((LongWritable) entry.getValue()).get());
                } else {
                    outValue.put(entry.getKey(), new LongWritable(
                            ((LongWritable) entry.getValue()).get()
                    ));
                }
            }
        }

        context.write(key, outValue);
    }
}
