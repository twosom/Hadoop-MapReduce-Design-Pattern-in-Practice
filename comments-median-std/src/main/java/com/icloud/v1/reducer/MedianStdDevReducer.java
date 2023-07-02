package com.icloud.v1.reducer;

import com.icloud.model.MedianStdDevTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class MedianStdDevReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevTuple> {

    private MedianStdDevTuple result = new MedianStdDevTuple();
    private List<Float> commentLengths = new ArrayList<>();

    @Override
    protected void reduce(IntWritable key,
                          Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        float sum = 0;
        float count = 0;
        commentLengths.clear();
        result.setStdDev(0);

        for (final IntWritable value : values) {
            commentLengths.add((float) value.get());
            sum += value.get();
            ++count;
        }
        Collections.sort(commentLengths);
        // 만약, commentLengths 가 짝수라면 중앙의 두 값 (count / 2 - 1), (count / 2) 두 값의 평균 계산
        // 홀수라면 그냥 가운데 값(count / 2) 를 중앙값으로 설정
        result.setMedian(calculateMedian(count));

        // 표준편차 계산
        float mean = sum / count;
        float sumOfSquares = 0.0f;
        for (Float length : commentLengths) {
            sumOfSquares += (length - mean) * (length - mean);
        }

        result.setStdDev((float) Math.sqrt(sumOfSquares / (count - 1)));
        context.write(key, result);
    }

    private float calculateMedian(float count) {
        if (count % 2 == 0)
            return (commentLengths.get((int) (count / 2 - 1)) + commentLengths.get((int) (count / 2))) / 2.0f;
        else return commentLengths.get((int) (count / 2));
    }
}
