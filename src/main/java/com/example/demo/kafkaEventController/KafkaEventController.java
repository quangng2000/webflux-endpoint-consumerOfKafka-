package com.example.demo.kafkaEventController;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.kafkaMessageListener.KafkaMessageListener;

import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/v1/events")
public class KafkaEventController {

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> getEvents() {
        return Flux.create(sink -> {
            KafkaMessageListener listener =
                    new KafkaMessageListener() {
                        @Override
                        public void onMessage(ConsumerRecord<String, String> data) {
                        	String message = String.format("%s.%s", data.key(), data.value());
                            sink.next(message);
                        }
                    };
            ContainerProperties containerProperties = new ContainerProperties("average-vital-signs-topic");
            containerProperties.setMessageListener(listener);
            ConcurrentMessageListenerContainer<String, String> container =
                    new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
            container.start();
            sink.onCancel(() -> {
                container.stop();
            });
        });
    }

}
