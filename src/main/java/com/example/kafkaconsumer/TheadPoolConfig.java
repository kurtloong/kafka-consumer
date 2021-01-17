package com.example.kafkaconsumer;

import kafka.utils.ShutdownableThread;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.concurrent.*;

@EnableAsync
@Configuration
public class TheadPoolConfig  {
    @Bean
    public ThreadPoolExecutor consumerThreadPool(){
        return new ThreadPoolExecutor(2,4,60,TimeUnit.MINUTES,new SynchronousQueue<>(),new ThreadPoolExecutor.CallerRunsPolicy());
    }

}
