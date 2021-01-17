package com.example.kafkaconsumer;


import lombok.Data;

import java.util.Date;

@Data
public class Video {
    /**
     * 视频发布者id
     */
    private String uid;
    /**
     * 视频id
     */
    private String videoId;

    /**
     * 最小点赞数
     */
    private Integer likeCount;

    /**
     * 视频分类
     */
    private String type;

    /**
     * 最小发布时间
     */
    private Date publishTime;

}
