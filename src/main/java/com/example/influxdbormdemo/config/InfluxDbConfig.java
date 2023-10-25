package com.example.influxdbormdemo.config;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InfluxDbConfig {

    @Value("${spring.influx.url:''}")
    private String url;

    @Value("${spring.influx.org:''}")
    private String org;

    @Value("${spring.influx.token:''}")
    private String token;

    @Value("${spring.influx.bucket:''}")
    private String bucket;

    @Bean
    public InfluxDBClient influxDB(){
        InfluxDBClient client = InfluxDBClientFactory.create(url, token.toCharArray(), org, bucket);
        return client;
    }
}