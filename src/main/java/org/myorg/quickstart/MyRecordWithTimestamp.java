package org.myorg.quickstart;

public class MyRecordWithTimestamp {
    public MyRecord myRecord;
    public Long timestamp;

    public MyRecordWithTimestamp() {
    }

    public MyRecordWithTimestamp(MyRecord myRecord, Long timestamp) {
        this.myRecord = myRecord;
        this.timestamp = timestamp;
    }
}
