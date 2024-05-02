package com.study.kafka.springkafka;

import com.study.kafka.KafkaServerConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
@RequiredArgsConstructor
public class KafkaProduceService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, MyMessage> jsonKafkaTemplate;

    public void send(String message) {
        kafkaTemplate.send(KafkaServerConfig.TOPIC_NAME, message);
    }

    public void sendWithCallback(String message) { // broker로 message 전송 후 Callback으로 받은 데이터
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(KafkaServerConfig.TOPIC_NAME, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Failed " + message + " due to : " + ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Sent " + message + " offset : " + result.getRecordMetadata().offset());
            }
        });
    }

    public void sendJson(MyMessage message) {
        jsonKafkaTemplate.send(KafkaServerConfig.TOPIC_NAME, message);
    }
}
