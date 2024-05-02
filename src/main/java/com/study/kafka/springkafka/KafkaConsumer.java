package com.study.kafka.springkafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.study.kafka.KafkaServerConfig;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    private ObjectMapper objectMapper = new ObjectMapper(); // String 형태의 Json 메시지를 MyMessage DTO로 변환

    @KafkaListener(topics = KafkaServerConfig.TOPIC_NAME) // 이거 하나로 ConsumerFactory로부터 토픽에 맞는 Consumer를 이용해 메시지를 consume 할 수 있다
    public void listenMessage(String jsonMessage) {
        try {
            MyMessage message = objectMapper.readValue(jsonMessage, MyMessage.class);
            System.out.println(">>>" + message.getName() + "," + message.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
