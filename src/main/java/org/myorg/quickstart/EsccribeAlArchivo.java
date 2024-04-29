package org.myorg.quickstart;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class EsccribeAlArchivo {

    public static void main(String[] args) throws IOException {

        Path path = Paths.get("/Users/edu/dev/projects/quickstart/src/main/resources/fichero.txt");
        ObjectMapper objectMapper = new ObjectMapper();
        TimestampedWord paco = new TimestampedWord("paco", System.currentTimeMillis());
        TimestampedWord edu = new TimestampedWord("edu", System.currentTimeMillis());
        TimestampedWord elvira = new TimestampedWord("elvira", System.currentTimeMillis());
        Files.write(path, objectMapper.writeValueAsBytes(paco));
        Files.write(path, objectMapper.writeValueAsBytes(edu));
        Files.write(path, objectMapper.writeValueAsBytes(elvira));
    }
}
