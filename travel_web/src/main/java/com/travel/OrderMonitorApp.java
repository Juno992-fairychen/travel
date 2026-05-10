package com.travel;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;


@MapperScan("com.travel.dao")
@SpringBootApplication
@ComponentScan(basePackages = {"com.travel"}) // 强制指定扫描 com.travel 下所有的 @Controller, @Service 等
public class OrderMonitorApp {
    public static void main(String[] args) {
        SpringApplication.run(OrderMonitorApp.class, args);
    }
}
