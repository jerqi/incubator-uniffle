package com.tecent.rss.client;

public interface WriterBuffer {

    // serialize should be happened here
    byte[] addRecord(Object key, Object value);

    byte[] clear();
}