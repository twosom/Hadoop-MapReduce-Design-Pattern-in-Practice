package com.icloud.v2.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

import static com.icloud.MRDPUtils.transformXmlToMap;

public class MedianStdDevMapper extends Mapper<LongWritable, Text, IntWritable, SortedMapWritable<IntWritable>> {

    private IntWritable commentLength = new IntWritable();
    private IntWritable outHour = new IntWritable();
    private static final LongWritable ONE = new LongWritable(1);

    @Override
    protected void map(LongWritable key,
                       Text value,
                       Context context) throws IOException, InterruptedException {
        Map<String, String> parsed = transformXmlToMap(value.toString());

        LocalDateTime creationDate = LocalDateTime.parse(parsed.get("CreationDate"));
        outHour.set(creationDate.getHour());

        String text = parsed.get("Text");
        commentLength.set(text.length());
        SortedMapWritable<IntWritable> outCommentLength = new SortedMapWritable<>();
        outCommentLength.put(commentLength, ONE);

        // 시간과 코멘트 길이 객체 출력
        context.write(outHour, outCommentLength);
    }
}
