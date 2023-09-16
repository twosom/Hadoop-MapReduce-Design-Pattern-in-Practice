package com.icloud.mapper;

import com.icloud.model.Comment;
import com.icloud.parser.impl.CommentParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

import java.io.IOException;

public class MediaStdDevMapper extends Mapper<LongWritable, Group, IntWritable, IntWritable> {

    private IntWritable outHour = new IntWritable();
    private IntWritable outCommentLength = new IntWritable();
    private CommentParser parser = new CommentParser();

    @Override
    protected void map(LongWritable key, Group value, Context context)
            throws IOException, InterruptedException {
        Comment comment = parser.parse(value);
        String text = comment.getText();
        int hour = comment.getCreationDate().getHour();
        this.outHour.set(hour);
        this.outCommentLength.set(text.length());

        context.write(outHour, outCommentLength);
    }
}
