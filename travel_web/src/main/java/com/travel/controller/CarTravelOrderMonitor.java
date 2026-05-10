package com.travel.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

import java.util.HashMap;
import java.util.Map;

//配置扫描的包路径

@Controller
public class CarTravelOrderMonitor {
    private final static Logger logger = LoggerFactory.getLogger(CarTravelOrderMonitor.class);

    /**
     * 项目首页
     * @return
     */
    @RequestMapping({"/", "/index", "/orderMonitoring", "/trackmonitor", "/station", "/movingway", "/userData", "heat"})
    public String home() {
        return "index";
    }

    @RequestMapping("/api/userData")
    @ResponseBody
    public Map<String, Object> userData() {
        // 这里就是给前端“喂”数据的地方
        Map<String, Object> map = new HashMap<>();
        map.put("status", "ok");
        map.put("code", 200);

        // 构造 data 内部结构
        Map<String, Object> data = new HashMap<>();
        data.put("userName", "管理员");
        data.put("loginTime", "2026-03-30");

        // 模拟一些看板可能需要的列表
        data.put("list", new java.util.ArrayList<>());

        map.put("data", data);
        return map;
    }

    public static void main(String[] args) {
        SpringApplication.run(CarTravelOrderMonitor.class,args);
    }

    @Configuration
    public static class WebSocketConfig {
        @Bean
        public ServerEndpointExporter serverEndpointExporter() {
            return new ServerEndpointExporter();
        }
    }
}
