package com.study.kafka.kafkaclients;

import com.study.kafka.KafkaServerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {
    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServerConfig.BOOTSTRAP_SERVER);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // String 메시지를 받을 것이므로 역직렬화 클래스는 StringSerializer 사용.
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        configs.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaServerConfig.GROUP_ID);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // CLI에서 사용한 --from-beginning 옵션. Consumer가 실행되기 전의 메시지부터 읽어온다.

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(KafkaServerConfig.TOPIC_NAME)); // Topic을 여러개 지정할 수도 있다.

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1)); // Consumer buffer에 데이터를 모으기 위한 시간 : 1초

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(">>>" + record);
            }
        }
    }
}