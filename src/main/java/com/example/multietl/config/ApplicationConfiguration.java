package com.example.multietl.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.nio.file.Path;

@Configuration
public class ApplicationConfiguration {

    @Bean
    public AppConfig appConfig() throws Exception {
        return new AppConfig(Path.of("app/config/config.yaml"));
    }

    @Bean
    public com.example.multietl.loader.DbLoader dbLoader(AppConfig config) {
        return new com.example.multietl.loader.DbLoader(
            config.getJdbcUrl(), 
            config.getJdbcUser(), 
            config.getJdbcPassword()
        );
    }

    @Bean
    public com.example.multietl.service.EtlService etlService(AppConfig config, com.example.multietl.loader.DbLoader dbLoader) {
        return new com.example.multietl.service.EtlService(config, dbLoader);
    }
}
