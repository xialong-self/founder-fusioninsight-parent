package com.founder;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.founder.fusioninsight.config.FusioninsightConfig;

@EnableScheduling
@ServletComponentScan
@SpringBootApplication
@EnableTransactionManagement
@EnableAspectJAutoProxy(exposeProxy = true,proxyTargetClass = true)
@MapperScan({"com.founder.*.*.dao","com.founder.*.dao"})
public class Application {
    public static void main(String[] args){
    	
    	FusioninsightConfig.initArgs(args);
        SpringApplication.run(Application.class,args);
    }
}
