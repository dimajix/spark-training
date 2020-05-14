package com.dimajix.training.hashtag2mysql.model.repo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.ZonedDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HashtagId implements Serializable {
    private ZonedDateTime ts;

    private String topic;
}
