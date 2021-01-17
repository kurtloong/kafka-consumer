package com.example.kafkaconsumer;

import java.io.Serializable;
import java.util.Date;

import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * nr_xinshi_subscribe_video
 * @author 
 */
@EqualsAndHashCode(callSuper = true)
@Data
@TableName(value = "nr_xinshi_subscribe_video")
public class NrXinshiSubscribeVideo extends Model<NrXinshiSubscribeVideo> implements Serializable  {
    /**
     * id
     */
    private Integer id;

    /**
     * 视频发布者id
     */
    private String uid;

    private Integer userId;

    /**
     * 订阅id
     */
    private Integer subscribeId;

    /**
     * 视频id
     */
    private String videoId;

    /**
     * 最小点赞数
     */
    private Integer likeCountMin;

    /**
     * 最大点赞数
     */
    private Integer likeCountMax;

    /**
     * 视频分类
     */
    private String type;

    /**
     * 最小评论数
     */
    private Integer commentCountMin;

    /**
     * 最大评论数
     */
    private Integer commentCountMax;

    /**
     * 最小阅读数
     */
    private Integer readCountMin;

    /**
     * 最大阅读数
     */
    private Integer readCountMax;

    /**
     * 最小收藏数
     */
    private Integer favCountMin;

    /**
     * 最大收藏数
     */
    private Integer favCountMax;

    /**
     * 最小发布时间
     */
    private Date publishTimeMin;

    /**
     * 最大发布时间
     */
    private Date publishTimeMax;

    /**
     * 通知消息状态 1开启微信消息0关闭
     */
    private Integer noticeStatus;

    /**
     * 监控状态 1开启0关闭
     */
    private Integer subscribeStatus;

    /**
     * 逻辑删除 0 未删除1 删除
     */
    private Boolean isDel;

    /**
     * gmt_create
     */
    private Date gmtCreate;

    /**
     * gmt_modify
     */
    private Date gmtModify;

    private static final long serialVersionUID = 1L;
}