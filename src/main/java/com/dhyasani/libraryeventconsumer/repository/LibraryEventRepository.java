package com.dhyasani.libraryeventconsumer.repository;

import com.dhyasani.libraryeventconsumer.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LibraryEventRepository extends CrudRepository<LibraryEvent,Integer> {
}
