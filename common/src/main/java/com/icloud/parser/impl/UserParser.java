package com.icloud.parser.impl;

import com.icloud.model.User;
import com.icloud.parser.Parser;
import org.apache.parquet.example.data.Group;

public class UserParser implements Parser<User> {

    @Override
    public User parse(Group group) {
        String location;
        try {
            location = group.getString("Location", 0);
        } catch (Exception e) {
            location = null;
        }

        return new User(location);
    }
}
