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

    public void setId(Integer id) {
        Id = id;
    }

    public LocalDateTime getCreationDate() {
        return CreationDate;
    }

    public void setCreationDate(LocalDateTime creationDate) {
        CreationDate = creationDate;
    }

    public Integer getPostId() {
        return PostId;
    }

    public void setPostId(Integer postId) {
        PostId = postId;
    }

    public Integer getScore() {
        return Score;
    }

    public void setScore(Integer score) {
        Score = score;
    }

    public String getText() {
        return Text;
    }

    public void setText(String text) {
        Text = text;
    }

    public Integer getUserId() {
        return UserId;
    }

    public void setUserId(Integer userId) {
        UserId = userId;
    }
}