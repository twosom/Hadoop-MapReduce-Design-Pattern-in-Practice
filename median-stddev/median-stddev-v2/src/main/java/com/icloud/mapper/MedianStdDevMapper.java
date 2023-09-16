package com.icloud.mapper;

import com.icloud.model.Comment;
import com.icloud.parser.impl.CommentParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

import java.io.IOException;

public class MedianStdDevMapper extends Mapper<LongWritable, Group, IntWritable, SortedMapWritable<IntWritable>> {

    private IntWritable commentLength = new IntWritable();
    private final static LongWritable ONE = new LongWritable(1);
    private IntWritable outHour = new IntWritable();
    private CommentParser parser = new CommentParser();

    @Override
    protected void map(LongWritable key, Group value, Context context)
            throws IOException, InterruptedException {
        Comment comment = parser.parse(value);

        int hour = comment.getCreationDate().getHour();
        outHour.set(hour);
        commentLength.set(comment.getText().length());

        SortedMapWritable<IntWritable> outCommentLength = new SortedMapWritable<>();
        outCommentLength.put(commentLength, ONE);

        context.write(outHour, outCommentLength);
    }
}
