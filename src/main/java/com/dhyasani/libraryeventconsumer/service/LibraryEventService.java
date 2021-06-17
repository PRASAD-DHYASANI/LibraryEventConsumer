package com.dhyasani.libraryeventconsumer.service;

import com.dhyasani.libraryeventconsumer.entity.LibraryEvent;
import com.dhyasani.libraryeventconsumer.repository.LibraryEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private LibraryEventRepository libraryEventRepository;



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
}
