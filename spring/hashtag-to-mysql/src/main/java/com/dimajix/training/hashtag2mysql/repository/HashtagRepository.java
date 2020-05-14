package com.dimajix.training.hashtag2mysql.repository;

import com.dimajix.training.hashtag2mysql.model.repo.HashtagEntry;
import com.dimajix.training.hashtag2mysql.model.repo.HashtagId;
import org.springframework.data.repository.CrudRepository;


public interface HashtagRepository extends CrudRepository<HashtagEntry, HashtagId> {
}
