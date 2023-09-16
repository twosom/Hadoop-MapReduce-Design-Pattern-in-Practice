package com.icloud.parser.impl;

import com.icloud.model.Comment;
import com.icloud.parser.Parser;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.api.Binary;

import java.time.LocalDateTime;

public class CommentParser implements Parser<Comment> {

    @Override
    public Comment parse(Group group) {
        final int Id = group.getInteger("Id", 0);
        final Binary binary = group.getInt96("CreationDate", 0);
        final NanoTime nanoTime = NanoTime.fromBinary(binary);
        final LocalDateTime CreationDate = fromParquetNanoTime(nanoTime);
        final int PostId = group.getInteger("PostId", 0);
        final int Score = group.getInteger("Score", 0);
        final String Text = group.getString("Text", 0);
        Integer UserId;
        try { //TODO 이 부분은 도저히 어쩔 수가 없음...
            UserId = group.getInteger("UserId", 0);
        } catch (Exception e) {
            UserId = null;
        }

        return new Comment(Id, CreationDate, PostId, Score, Text, UserId);
    }
}
