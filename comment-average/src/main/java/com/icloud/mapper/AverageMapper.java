package com.icloud.mapper;

import com.icloud.model.Comment;
import com.icloud.parser.CommentParser;
import com.icloud.writable.CountAverageTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class AverageMapper extends Mapper<LongWritable, Group, IntWritable, CountAverageTuple> {

    private IntWritable outHour = new IntWritable();
    private CountAverageTuple outCountAverage = new CountAverageTuple();
    private static final SimpleDateFormat sdf =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private CommentParser parser = new CommentParser();
    @Override
    protected void map(LongWritable key, Group value, Context context)
            throws IOException, InterruptedException {
        Comment comment = parser.parse(value);
        int hour = comment.getCreationDate().getHour();
        outHour.set(hour);

        String text = comment.getText();

        // 코멘트 길이 추출
        outCountAverage.setCount(1);
        outCountAverage.setAverage(text.length());

        context.write(outHour, outCountAverage);
    }
}
