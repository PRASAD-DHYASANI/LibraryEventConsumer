package com.dhyasani.libraryeventconsumer.consumer;

import com.dhyasani.libraryeventconsumer.service.LibraryEventService;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventConsumer {

    @Autowired
    private LibraryEventService libraryEventService;


    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer,String> record) throws JsonProcessingException {
        log.info("Consumer Message : {}", record);
        libraryEventService.processLibraryEvent(record);
    }
}
