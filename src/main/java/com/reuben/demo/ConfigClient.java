package com.reuben.demo;

import ch.qos.logback.classic.Level;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;


/**
 * 长轮询客户端,负责对进行长轮询请求以及对获取到更新配置时进行配置更改事件发布
 * @date 2021-02-19
 * @link https://github.com/lexburner/longPolling-demo/blob/main/src/main/java/moe/cnkirito/demo/ConfigClient.java
 */
@Slf4j
public class ConfigClient {

    // http客户端
    private CloseableHttpClient httpClient;
    // http请求配置
    private RequestConfig requestConfig;
    // 是否停止长轮询
    private static volatile boolean polling = true;

    public ConfigClient() {
        this.httpClient = HttpClientBuilder.create().build();
        this.requestConfig = RequestConfig.custom().setSocketTimeout(40000).build();
    }

    //长连接请求
    @SneakyThrows
    public CloseableHttpResponse longPolling(String endpoint){
        HttpGet get = new HttpGet(endpoint);
        CloseableHttpResponse response = httpClient.execute(get);
        return response;
    }

    @SneakyThrows
    public static void executePolling(ConfigClient configClient, String dataId, String url){
        Thread.sleep(2000);
        Integer errorNum = 0;
        String endpoint = url.concat("?dataId=").concat(dataId);
        while(polling) {
            log.info("start polling");
            CloseableHttpResponse response = configClient.longPolling(endpoint);
            switch (response.getStatusLine().getStatusCode()) {
                case 200:
                    BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                    StringBuilder result = new StringBuilder();
                    String line;
                    while((line = reader.readLine()) != null){
                        result.append(line);
                    }
                    reader.close();
                    String config = result.toString();
                    log.info("longPolling dataId: [{}], receive configInfo: {}", dataId, config);
                    if(errorNum > 0) errorNum--;
                    break;
                case 304:
                    log.info("longPolling dataId: [{}] once finished, configInfo is unchanged, longPolling again", dataId);
                    if(errorNum > 0) errorNum--;
                    break;
                default:
                    errorNum++;
                    log.error("unExcepted HTTP status code {}", response.getStatusLine().getStatusCode());
                    if(errorNum > 5) {
                        polling = false;
                        log.error("unExcepted HTTP status code, stop polling");
                    }
            }
            response.close();
        }
    }


    public static void main(String[] args) {

        Logger logger = (Logger)LoggerFactory.getLogger("org.apache.http");
        logger.setLevel(Level.INFO);
        logger.setAdditive(false);

        String dataId = "sys-cfg.yml";
        String url = "http://localhost:8080/listener";
        ConfigClient configClient = new ConfigClient();
        executePolling(configClient, dataId, url);
    }
}
