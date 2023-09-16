package com.icloud.reducer;

import com.icloud.writable.MedianStdDevTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MedianStdDevReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevTuple> {

    private MedianStdDevTuple result = new MedianStdDevTuple();
    private List<Float> commentLenths = new ArrayList<>();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        float sum = 0;
        float count = 0;
        commentLenths.clear();
        result.setStdDev(0);

        // 해당 키에 대한모든 입력 값 순환 처리
        for (IntWritable value : values) {
            commentLenths.add((float) value.get());
            sum += value.get();
            ++count;
        }
        Collections.sort(commentLenths);
        float median = calculateMedian(count);
        result.setMedian(median);

        float mean = sum / count;
        float sumOfSquares = 0.0f;
        for (float length : commentLenths) {
            sumOfSquares += (length - mean) * (length - mean);
        }

        result.setStdDev((float) Math.sqrt(sumOfSquares / (count - 1)));
        context.write(key, result);
    }

    private float calculateMedian(float count) {
        int pivot = (int) (count / 2);
        // 짝수면 중앙의 두 값의 평균 계산
        if (count % 2 == 0) {
            float before = commentLenths.get(pivot - 1);
            float after = commentLenths.get(pivot);
            return (before + after) / 2.0f;
        } else { // 홀수면 가운데 값
            return commentLenths.get(pivot);
        }
    }
}
