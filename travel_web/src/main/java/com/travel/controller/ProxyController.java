package com.travel.controller;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.util.stream.Collectors;

@RestController
public class ProxyController {

    // 代理前端请求到正确的后端
    @RequestMapping("/api/**")
    public String proxy(HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI();
        String method = request.getMethod();

        // 读取请求体
        String body = "";
        if (request.getReader() != null) {
            body = request.getReader().lines().collect(Collectors.joining());
        }

        // 转发到本地正确的接口
        String targetUrl = "http://localhost:8080" + uri.replace("/api", "");

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            if ("POST".equalsIgnoreCase(method)) {
                HttpPost httpPost = new HttpPost(targetUrl);
                httpPost.setHeader("Content-Type", "application/json");
                httpPost.setEntity(new StringEntity(body, "UTF-8"));
                CloseableHttpResponse response = httpClient.execute(httpPost);
                return EntityUtils.toString(response.getEntity());
            } else {
                HttpGet httpGet = new HttpGet(targetUrl);
                CloseableHttpResponse response = httpClient.execute(httpGet);
                return EntityUtils.toString(response.getEntity());
            }
        }
    }
}