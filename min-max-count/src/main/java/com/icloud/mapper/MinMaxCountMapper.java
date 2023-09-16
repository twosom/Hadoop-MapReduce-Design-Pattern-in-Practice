package com.icloud.mapper;

import com.icloud.model.Comment;
import com.icloud.parser.impl.CommentParser;
import com.icloud.writable.MinMaxCountTuple;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class MinMaxCountMapper extends Mapper<LongWritable, Group, Text, MinMaxCountTuple> {

    private Text outUserId = new Text();
    private MinMaxCountTuple outTuple = new MinMaxCountTuple();
    private CommentParser parser = new CommentParser();


    @Override
    protected void map(LongWritable key, Group value, Context context)
            throws IOException, InterruptedException {
        Comment comment = parser.parse(value);
        //TODO
        // 만약 UserID 가 없는 경우에는 패스
        if (comment.getUserId() == null) return;
        LocalDateTime creationDate = comment.getCreationDate();
        //TODO
        // 일단 CreationDate 로 최솟값, 최댓값 설정
        Instant instant = creationDate.atZone(ZoneId.systemDefault()).toInstant();
        Date date = Date.from(instant);

        outTuple.setMin(date);
        outTuple.setMax(date);
        outTuple.setCount(1);
        outUserId.set(String.valueOf(comment.getUserId()));

        context.write(outUserId, outTuple);
    }
}
