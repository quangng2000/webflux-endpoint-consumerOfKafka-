package com.example.demo.kafkaMessageListener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

@Component
public class KafkaMessageListener implements MessageListener<String, String> {

    private final Flux<String> messageFlux;
    private FluxSink<String> messageSink;

    public KafkaMessageListener() {
        messageFlux = Flux.<String>create(sink -> messageSink = sink)
                .share() // Make the flux hot
                .publishOn(Schedulers.immediate()) // Emit items immediately on the current thread
                .timeout(Duration.ofSeconds(5)) // Timeout if no subscriber is available
                .onBackpressureLatest(); // Discard old items if a subscriber is not fast enough
    }

    public Flux<String> getMessageFlux() {
        return messageFlux;
    }

    @Override
    public void onMessage(ConsumerRecord<String, String> record) {
        // Emit the message to the flux
        messageSink.next(record.value());
    }

}
