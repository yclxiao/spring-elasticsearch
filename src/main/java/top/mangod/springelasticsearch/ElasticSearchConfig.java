package top.mangod.springelasticsearch;

import lombok.Data;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.util.Arrays;

/**
 * ES的配置类
 * ElasticSearchConfig
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "elasticsearch")
public class ElasticSearchConfig {

    private String hosts;
    private Integer port;

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        HttpHost[] httpHosts = Arrays.stream(hosts.split(","))
                .filter(e -> !StringUtils.isEmpty(e))
                .map(e -> new HttpHost(e, port, "http"))
                .toArray(HttpHost[]::new);

        return new RestHighLevelClient(
                RestClient.builder(
                        httpHosts
                )
        );
    }
}
