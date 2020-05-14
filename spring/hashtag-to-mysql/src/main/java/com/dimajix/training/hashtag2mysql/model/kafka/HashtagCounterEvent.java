package com.dimajix.training.hashtag2mysql.model.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HashtagCounterEvent {
    @JsonProperty("window")
    private TimeWindow window;

    @JsonProperty("topic")
    private String topic;

    @JsonProperty("count")
    private int count;
}
