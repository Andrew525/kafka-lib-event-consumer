package com.kafkalibevent.example.jpa;

import com.kafkalibevent.example.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

public interface LibraryEventRepository extends CrudRepository<LibraryEvent, Integer> {


}
