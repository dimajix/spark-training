package com.dimajix.training.hashtag2mysql.model.repo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import java.time.ZonedDateTime;

@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
@IdClass(HashtagId.class)
public class HashtagEntry {
    @Id
    private ZonedDateTime ts;

    @Id
    private String topic;

    private long count;
}
