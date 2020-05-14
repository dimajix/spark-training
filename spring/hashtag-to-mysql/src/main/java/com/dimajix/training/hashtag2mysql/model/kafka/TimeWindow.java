package com.dimajix.training.hashtag2mysql.model.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.ZonedDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TimeWindow {
    @JsonProperty("start")
    private ZonedDateTime start;

    @JsonProperty("end")
    private ZonedDateTime end;
}
