package com.dhyasani.libraryeventconsumer.consumer;

import com.dhyasani.libraryeventconsumer.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

//@Component
@Slf4j
public class LibraryEventManualAckConsumer implements AcknowledgingMessageListener<Integer,String> {

    @Autowired
    private LibraryEventService libraryEventService;

    @Override
    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer, String> record, Acknowledgment acknowledgment) {
        log.info("Consumer Message : {}", record);
        //libraryEventService.processLibraryEvent(record);
        acknowledgment.acknowledge();
    }




    /*@KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer,String> record) throws JsonProcessingException {
        log.info("Consumer Message : {}", record);
        libraryEventService.processLibraryEvent(record);
    }*/


}
