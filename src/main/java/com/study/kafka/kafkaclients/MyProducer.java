package com.study.kafka.kafkaclients;

import com.study.kafka.KafkaServerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServerConfig.BOOTSTRAP_SERVER);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // String 메시지를 전송할 것이므로 직렬화 클래스는 StringSerializer 사용.
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.RETRIES_CONFIG, "100");

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String message = "First Message";
        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaServerConfig.TOPIC_NAME, message);
        RecordMetadata recordMetadata = producer.send(record).get(); // RecordMetadata에서 파티션, 오프셋 정보 등을 확인가능
        System.out.printf(">>> %s, %d, %d", message, recordMetadata.partition(), recordMetadata.offset());

        producer.flush();
        producer.close();
    }
}
