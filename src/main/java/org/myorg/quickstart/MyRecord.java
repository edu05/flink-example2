package org.myorg.quickstart;

public class MyRecord {
    public String clientId;
    public String feature1;
    public String feature2;

    public MyRecord() {}

    public MyRecord(String clientId, String feature1, String feature2) {
        this.clientId = clientId;
        this.feature1 = feature1;
        this.feature2 = feature2;
    }

    public static MyRecord f1(String clientId, String feature1) {
        return new MyRecord(clientId, feature1, null);
    }

    public static MyRecord f2(String clientId, String feature2) {
        return new MyRecord(clientId, null, feature2);
    }

    public String clientId() {
        return clientId;
    }

    @Override
    public String toString() {
        return "{" +
                "clientId='" + clientId + '\'' +
                ", f1='" + feature1 + '\'' +
                ", f2='" + feature2 + '\'' +
                '}';
    }
}
