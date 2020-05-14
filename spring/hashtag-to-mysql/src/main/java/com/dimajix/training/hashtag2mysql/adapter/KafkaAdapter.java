package com.dimajix.training.hashtag2mysql.adapter;

import com.dimajix.training.hashtag2mysql.model.kafka.HashtagCounterEvent;
import com.dimajix.training.hashtag2mysql.model.repo.HashtagEntry;
import com.dimajix.training.hashtag2mysql.repository.HashtagRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
@Slf4j
@AllArgsConstructor
public class KafkaAdapter {
    private final HashtagRepository repository;

    @KafkaListener(id = "hashtag2mysql", topics = "hashtags")
    public void receiveEvent(HashtagCounterEvent event) {
        HashtagEntry entry = new HashtagEntry(
                event.getWindow().getStart(),
                event.getTopic(),
                event.getCount()
            );
        repository.save(entry);
    }
}
