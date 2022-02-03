package com.atstec;

import static org.junit.jupiter.api.Assertions.*;

class BulkLoadTest {

    @org.junit.jupiter.api.Test
    void main() {
        String[] myStringArray;
        myStringArray = new String[]{
            "/Users/viktormolokanov/IdeaProjects/generate-cassandra-sstables/src/test/resources/order_status_change_t_sample.csv",
            "/Users/viktormolokanov/IdeaProjects/generate-cassandra-sstables/target"};
        BulkLoad.main(myStringArray);
    }
}