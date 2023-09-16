package com.icloud.reducer;

import com.icloud.writable.MedianStdDevTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

public class MedianStdDevReducer
        extends Reducer<IntWritable, SortedMapWritable<IntWritable>, IntWritable, MedianStdDevTuple> {

    private MedianStdDevTuple result = new MedianStdDevTuple();
    private TreeMap<Integer, Long> commentLengthCounts = new TreeMap<>();

    @Override
    protected void reduce(IntWritable key, Iterable<SortedMapWritable<IntWritable>> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        long totalComments = 0;
        commentLengthCounts.clear();
        result.setMedian(0);
        result.setStdDev(0);

        // Map Task 에서 넘어온 해당 연도에 대한 모든 값(댓글 길이, 갯수 쌍인 맵)들을 순회
        // 즉, List<Map> 에서 List 를 순회하여 각 Map을 하나의 Map 객체로 합치는 과정이라고 볼 수 있음.
        for (SortedMapWritable<IntWritable> value : values) {
            // key => length, value => count
            for (Entry<IntWritable, Writable> entry : value.entrySet()) {
                int length = entry.getKey().get();
                long count = ((LongWritable) entry.getValue()).get();

                // 해당 댓글 길이의 총 갯수 누적
                totalComments += count;
                // 해당 댓글 길이와 갯수를 곱해서 총 댓글 길이 저장
                sum += length * count;
                // 만약 length 키의 값이 없으면 count 저장
                // 있다면 count 와 merge
                commentLengthCounts.merge(length, count, Long::sum);
            }
        }

        long medianIndex = totalComments / 2L;
        long previousComments = 0;
        long comments = 0;
        int prevKey = 0;

        for (Entry<Integer, Long> entry : commentLengthCounts.entrySet()) {
            Long commentsCount = entry.getValue();
            Integer commentLength = entry.getKey();

            comments = previousComments + commentsCount;
            if ((previousComments <= medianIndex) && (medianIndex < comments)) {
                float median = (totalComments % 2 == 0) && (previousComments == medianIndex) ?
                        (commentLength + prevKey) / 2.0f :
                        commentLength;
                result.setMedian(median);
                break;
            }

            previousComments = comments;
            prevKey = commentLength;
        }

        float mean = sum / totalComments;

        float sumOfSquares = 0.0f;
        for (Entry<Integer, Long> entry : commentLengthCounts.entrySet()) {
            sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) * entry.getValue();
        }
        result.setStdDev((float) Math.sqrt(sumOfSquares / (totalComments - 1)));
        context.write(key, result);
    }
}
