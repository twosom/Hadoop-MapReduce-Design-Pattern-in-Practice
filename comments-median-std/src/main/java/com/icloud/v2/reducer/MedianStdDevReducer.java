package com.icloud.v2.reducer;

import com.icloud.model.MedianStdDevTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class MedianStdDevReducer extends Reducer<IntWritable, SortedMapWritable<IntWritable>, IntWritable, MedianStdDevTuple> {

    private MedianStdDevTuple result = new MedianStdDevTuple();
    private Map<Integer, Long> commentsLengthCounts = new TreeMap<>();

    @Override
    protected void reduce(IntWritable key,
                          Iterable<SortedMapWritable<IntWritable>> values,
                          Context context)
            throws IOException, InterruptedException {
        float sum = 0;
        long totalComments = 0;
        commentsLengthCounts.clear();
        result.setMedian(0);
        result.setStdDev(0);


        for (final Map<IntWritable, Writable> value : values) {
            for (final Map.Entry<IntWritable, Writable> entry : value.entrySet()) {
                int length = entry.getKey().get();
                long count = ((LongWritable) entry.getValue()).get();

                totalComments += count;
                sum += length * count;

                Long storedCount = commentsLengthCounts.get(length);
                if (storedCount == null) commentsLengthCounts.put(length, count);
                else commentsLengthCounts.put(length, storedCount + 1);
            }
        }

        long medianIndex = totalComments / 2L;
        long previousComments = 0;
        long comments = 0;
        long prevKey = 0;
        for (final Map.Entry<Integer, Long> entry : commentsLengthCounts.entrySet()) {
            comments = entry.getValue();
            if (previousComments <= medianIndex && medianIndex < comments) {
                float median = calculateMedian(totalComments, medianIndex, previousComments, prevKey, entry.getKey());
                result.setMedian(median);
                break;
            }
            previousComments = comments;
            prevKey = entry.getKey();
        }

        float mean = sum / totalComments;

        float sumOfSquares = 0.0f;
        for (final Map.Entry<Integer, Long> entry : commentsLengthCounts.entrySet()) {
            sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) * entry.getValue();
        }

        result.setStdDev((float) Math.sqrt(sumOfSquares / (totalComments - 1)));
        context.write(key, result);
    }

    private float calculateMedian(long totalComments,
                                  long medianIndex,
                                  long previousComments,
                                  long prevKey,
                                  int entryKey) {
        if (totalComments % 2 == 0 && previousComments == medianIndex) return (entryKey + prevKey) / 2.0f;
        return entryKey;
    }
}
