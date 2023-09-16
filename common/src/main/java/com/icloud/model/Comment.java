package com.icloud.model;

import java.time.LocalDateTime;

public class Comment {
    private Integer Id;
    private LocalDateTime CreationDate;
    private Integer PostId;
    private Integer Score;
    private String Text;
    private Integer UserId;

    public Comment(Integer id, LocalDateTime creationDate,
                   Integer postId, Integer score, String text, Integer userId) {
        Id = id;
        CreationDate = creationDate;
        PostId = postId;
        Score = score;
        Text = text;
        UserId = userId;
    }

    public Integer getId() {
        return Id;
    }

    public LocalDateTime getCreationDate() {
        return CreationDate;
    }

    public Integer getPostId() {
        return PostId;
    }

    public Integer getScore() {
        return Score;
    }

    public String getText() {
        return Text;
    }

    public Integer getUserId() {
        return UserId;
    }
}