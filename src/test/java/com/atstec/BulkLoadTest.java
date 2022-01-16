package com.atstec;

import static org.junit.jupiter.api.Assertions.*;

class BulkLoadTest {

    @org.junit.jupiter.api.Test
    void main() {
        String[] myStringArray;
        myStringArray = new String[]{"C:\\Users\\moloc\\OneDrive\\Рабочий стол\\20220108043252.csv", "C:\\Users\\moloc\\IdeaProjects\\create_sstables\\target"};
        BulkLoad.main(myStringArray);
    }
}