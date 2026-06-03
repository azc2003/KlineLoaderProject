package com.example.consumer.config;

import com.example.consumer.model.Kline;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String baseGroupId;

    /** 公共属性，groupId / 批参数由调用方指定 */
    private Map<String, Object> props(String groupId, int maxPoll, int fetchMinBytes, int fetchMaxWaitMs) {
        Map<String, Object> p = new HashMap<>();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);
        p.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        p.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
        p.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        p.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        p.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.example.consumer.model.Kline");
        return p;
    }

    private ConcurrentKafkaListenerContainerFactory<String, Kline> build(Map<String, Object> props) {
        ConcurrentKafkaListenerContainerFactory<String, Kline> f =
            new ConcurrentKafkaListenerContainerFactory<>();
        f.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        f.setBatchListener(true);
        f.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        // concurrency 默认 1：每个 topic 1 partition，单线程消费 → 保证分区内有序
        return f;
    }

    /** Realtime：低延迟 —— 不攒批(fetch.min.bytes=1)，最多等100ms，小批 */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Kline> realtimeFactory() {
        return build(props(baseGroupId + "-realtime", 500, 1, 100));
    }

    /** Backfill：高吞吐 —— 攒满100KB或500ms，大批4000 */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Kline> backfillFactory() {
        return build(props(baseGroupId + "-backfill", 4000, 102400, 500));
    }
}
