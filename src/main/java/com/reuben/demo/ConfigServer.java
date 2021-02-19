package com.reuben.demo;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 长轮询服务端,负责对请求保持、超时返回以及配置改动后立即响应
 * @date 2021-02-19
 * @link https://github.com/lexburner/longPolling-demo/blob/main/src/main/java/moe/cnkirito/demo/ConfigServer.java
 */
@RestController
@Slf4j
@SpringBootApplication
public class ConfigServer {

    @Data
    private static class AsyncTask{
        // 长轮询请求上下文,包含请求和响应体
        private AsyncContext asyncContext;
        // 超时响应标记
        private boolean  timeout;

        public AsyncTask(AsyncContext asyncContext, boolean timeout) {
            this.asyncContext = asyncContext;
            this.timeout = timeout;
        }
    }

    // 单Key多Value映射Map,保存请求拉取任务
    private volatile Multimap<String, AsyncTask> dataIdContext = Multimaps.synchronizedSetMultimap(HashMultimap.create());

    // 线程工厂定义,指定各线程名称
    private ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("longPolling-timeout-checker-%d").build();

    // 计划线程池,用作持有请求指定超时时长
    private ScheduledExecutorService timeoutChecker = new ScheduledThreadPoolExecutor(1, threadFactory);

    // ① 监听接入点
    @RequestMapping("/listener")
    public void addListener(HttpServletRequest request, HttpServletResponse response){
        //只支持单dataId
        String dataId = request.getParameter("dataId");

        // ② 开启异步
        AsyncContext asyncContext = request.startAsync(request, response);
        AsyncTask asyncTask = new AsyncTask(asyncContext, true);

        dataIdContext.put(dataId, asyncTask);

        // ③ 启动定时器，30s 后写入 304 响应. 新开线程hole住该次请求
        timeoutChecker.schedule(() ->{
            if(asyncTask.isTimeout()){
                dataIdContext.remove(dataId, asyncTask);
                response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
                try {
                    response.getWriter().println("no-change");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                asyncContext.complete();
            }
        }, 30000 , TimeUnit.MILLISECONDS);
    }

    // ④ 配置发布接入点
    @RequestMapping("/publishConfig")
    @SneakyThrows
    public String publishConfig(String dataId, String configInfo){
        log.info("publish config dataId[{}], configInfo: {}", dataId, configInfo);

        Collection<AsyncTask> asyncTasks = dataIdContext.removeAll(dataId);

        for(AsyncTask task : asyncTasks){
            task.setTimeout(false);
            HttpServletResponse response = (HttpServletResponse)task.getAsyncContext().getResponse();
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println(configInfo);
            task.getAsyncContext().complete();
        }
        return "success";
    }

    public static void main(String[] args) {
        SpringApplication.run(ConfigServer.class, args);
    }
}
