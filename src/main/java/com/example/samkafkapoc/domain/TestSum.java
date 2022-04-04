package com.example.samkafkapoc.domain;

import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;

@Document
public class TestSum {
    @MongoId
    public String id;

    @Indexed
    public String name;
    public long sum;
}
