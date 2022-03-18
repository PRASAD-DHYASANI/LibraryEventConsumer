package com.dhyasani.libraryeventconsumer.service;

import com.dhyasani.libraryeventconsumer.entity.LibraryEvent;
import com.dhyasani.libraryeventconsumer.repository.LibraryEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;



    public void processLibraryEvent(ConsumerRecord<Integer,String> record) throws JsonProcessingException {
            LibraryEvent libraryEvent = objectMapper.readValue(record.value(), LibraryEvent.class);

            switch(libraryEvent.getLibraryEventType()){
                case NEW:
                    save(libraryEvent);
                    break;
                case UPDATE:
                    validate(libraryEvent);
                    save(libraryEvent);
                    break;
                default:
                    log.error("Library Event type not supported by system");
            }
    }

    private void validate(LibraryEvent libraryEvent) {
        if(libraryEvent.getLibraryEventId() == null){
            throw new IllegalArgumentException("Library Event Id is mandatory field");
        }
        if(libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 000){
            throw new RecoverableDataAccessException("recoverable Error occurred");
        }
        Optional<LibraryEvent> event = libraryEventRepository.findById(libraryEvent.getLibraryEventId());
        if(event.isEmpty()){
            throw new IllegalArgumentException("provided Library Event Id is not valid");
        }
        log.info("Validation Successful for the Library Event : {}",event.get());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);
        log.info("Successfuly persisted Library Event : {} ",libraryEvent);
    }

    public void handleRecovery(ConsumerRecord<Integer,String> record){
        Integer key = record.key();
        String value = record.value();

        ListenableFuture<SendResult<Integer,String>> future  = kafkaTemplate.sendDefault(key,value);
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                handleFailure(key,value,throwable);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key,value, result);
            }
        });
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error sending message : {} " , ex.getMessage());

        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Error in OnFailure: {} ", throwable.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent successfully for the key : {} and value : {}, partition is {}",key,value,result.getRecordMetadata().partition());
    }
}
