package com.icloud.model;

import java.time.LocalDateTime;

public class User {
    private String Location;

    public User(String location) {
        Location = location;
    }

    public String getLocation() {
        return Location;
    }
}
