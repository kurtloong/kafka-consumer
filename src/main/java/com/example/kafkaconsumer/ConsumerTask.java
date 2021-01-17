package com.example.kafkaconsumer;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.kafkaconsumer.mapper.NrXinshiSubscribeVideoMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.SynchronousQueue;

@Component
@Slf4j
public class ConsumerTask {

    @Autowired
    private WorkTask workTask;
    @Autowired
    private TheadPoolConfig theadPoolConfig;

    public static final SynchronousQueue<ConsumerRecord<?, ?>> PIP_QUEUE = new SynchronousQueue<>();


    @KafkaListener(topics = "test")
    public void onMessage3(List<ConsumerRecord<?, ?>> records, Acknowledgment acknowledgment) {
        System.out.println(">>>批量消费一次，records.size()="+records.size());
        for (ConsumerRecord<?, ?> record : records) {
            theadPoolConfig.consumerThreadPool().execute(()->{
                log.info("提交到QUEUE：{}",record.value());
                try {
                    PIP_QUEUE.put(record);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.info("返回offset");
                acknowledgment.acknowledge();
            });
            log.info("执行线程");
            workTask.process();
        }
    }
}
