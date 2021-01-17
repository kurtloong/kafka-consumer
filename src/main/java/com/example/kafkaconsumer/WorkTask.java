package com.example.kafkaconsumer;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.extra.spring.SpringUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.kafkaconsumer.mapper.NrXinshiSubscribeVideoMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.SynchronousQueue;

import static com.example.kafkaconsumer.ConsumerTask.PIP_QUEUE;

@Slf4j
@Component
public class WorkTask {
    @Autowired
    private NrXinshiSubscribeVideoMapper dao;

    @Async("consumerThreadPool")
    public void process(){
        Video video = JSONUtil.toBean(Objects.requireNonNull(PIP_QUEUE.poll()).value().toString(),Video.class);
        log.info("消费的数据：{}",JSONUtil.toJsonStr(video));
        LambdaQueryWrapper<NrXinshiSubscribeVideo> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(NrXinshiSubscribeVideo::getIsDel,0);
        queryWrapper.in(NrXinshiSubscribeVideo::getUid,"-1",video.getUid());
        queryWrapper.in(NrXinshiSubscribeVideo::getVideoId,"-1",video.getVideoId());
        queryWrapper.in(NrXinshiSubscribeVideo::getType,"-1",video.getType());
        queryWrapper.le(NrXinshiSubscribeVideo::getLikeCountMin,video.getLikeCount());
        queryWrapper.ge(NrXinshiSubscribeVideo::getLikeCountMax,video.getLikeCount());
        List<NrXinshiSubscribeVideo> videoList = dao.selectList(queryWrapper);
        log.info("size:{}",videoList.size());
    }
}
