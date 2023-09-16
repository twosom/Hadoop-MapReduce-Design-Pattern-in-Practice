package com.icloud.model;

public class Post {
    private Integer Id;

    private String Body;

    private Integer PostTypeId;

    public Integer getId() {
        return Id;
    }

    public String getBody() {
        return Body;
    }

    public Integer getPostTypeId() {
        return PostTypeId;
    }

    public Post(Integer id, String body, Integer postTypeId) {
        Id = id;
        Body = body;
        PostTypeId = postTypeId;
    }

    public boolean isPost() {
        return this.PostTypeId != null &&
               this.PostTypeId == 1; // PostTypeId가 1이면 게시글, 2이면 답글
    }
}
