package com.travel.controller;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOriginPatterns("*")
                .allowedMethods("POST", "GET", "PUT", "OPTIONS", "DELETE")
                .maxAge(3600)
                .allowCredentials(true);
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/trajectory").setViewName("forward:/index.html");
        registry.addViewController("/orderMonitoring").setViewName("forward:/index.html");
        registry.addViewController("/station").setViewName("forward:/index.html");
        registry.addViewController("/movingway").setViewName("forward:/index.html");
        registry.addViewController("/userData").setViewName("forward:/index.html");
        registry.addViewController("/heatMap").setViewName("forward:/index.html");
        registry.addViewController("/system").setViewName("forward:/index.html");
    }
}
