package com.dhyasani.libraryeventconsumer.entity;

import lombok.*;

import javax.persistence.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class LibraryEvent {

    @Id
    @GeneratedValue(strategy=GenerationType.SEQUENCE)
    private Integer libraryEventId;
    @Enumerated(EnumType.STRING)
    private LibraryEventType libraryEventType;

    @OneToOne(mappedBy = "libraryEvent",cascade = {CascadeType.ALL})
    @ToString.Exclude
    private Book book;

}
