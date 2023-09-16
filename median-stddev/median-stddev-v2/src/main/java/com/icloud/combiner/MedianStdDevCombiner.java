package com.icloud.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;


public class MedianStdDevCombiner
        extends Reducer<IntWritable, SortedMapWritable<IntWritable>, IntWritable, SortedMapWritable<IntWritable>> {
    private final SortedMapWritable<IntWritable> result = new SortedMapWritable<>();

    @Override
    protected void reduce(IntWritable key, Iterable<SortedMapWritable<IntWritable>> values, Context context)
            throws IOException, InterruptedException {
        for (SortedMapWritable<IntWritable> value : values) {
            for (Map.Entry<IntWritable, Writable> entry : value.entrySet()) {
                IntWritable commentsLength = entry.getKey();
                LongWritable commentsCount = (LongWritable) entry.getValue();

                LongWritable resultCount = (LongWritable) result.get(commentsLength);
                if (resultCount != null)
                    resultCount.set(resultCount.get() + commentsCount.get());
                else
                    result.put(commentsLength, new LongWritable(commentsCount.get()));
            }
        }
        context.write(key, result);
    }
}
