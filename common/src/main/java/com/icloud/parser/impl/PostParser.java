package com.icloud.parser.impl;

import com.icloud.model.Post;
import com.icloud.parser.Parser;
import org.apache.parquet.example.data.Group;

public class PostParser implements Parser<Post> {

    @Override
    public Post parse(Group group) {
        final int Id = group.getInteger("Id", 0);
        final String Body = group.getString("Body", 0);
        final int PostTypeId = group.getInteger("PostTypeId", 0);

        return new Post(Id, Body, PostTypeId);
    }
}
