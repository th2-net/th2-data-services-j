package com.exactpro.th2.dataservices;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.exactpro.th2.dataservices.DataServicesUtils.*;

public class TestData {

    private static Data generalData;

    private static Path tempPath;
    private static File tempFile;

    @TempDir
    static Path tempDir;

    @BeforeAll
    public static void setUp() {
        generalData = ConfTest.generalData();
        tempPath = tempDir.resolve("test.txt");
        tempFile = tempPath.toFile();
    }

    @Test
    public void testIterData() {
        long counter = 0;
        for (JsonElement ignored : generalData) {
            counter++;
        }
        Assertions.assertEquals(counter, 21L);
    }

    @Test
    public void testLenData() {
        Assertions.assertEquals(generalData.getLength(), 21);
    }

    @Test
    public void testFilterData() throws IOException {
        Data newData = generalData.filter(record -> record.getAsJsonObject().get(BATCH_ID) == null);
        Assertions.assertEquals(newData.getLength(), 9);
    }

    @Test
    public void testMapDataTransform() throws IOException {
        Data newData = generalData.map(record -> record.getAsJsonObject().get(EVENT_TYPE));
        TreeSet<String> dataEventTypes = newData.getData()
                .stream()
                .map(JsonElement::getAsString).sorted().collect(Collectors.toCollection(TreeSet::new));
        TreeSet<String> eventTypes = new TreeSet<>();
        eventTypes.add("");
        eventTypes.add("placeOrderFIX");
        eventTypes.add("Send message");
        eventTypes.add("Checkpoint");
        eventTypes.add("Checkpoint for session");
        eventTypes.add("message");
        eventTypes.add("Outgoing message");
        Assertions.assertIterableEquals(dataEventTypes, eventTypes);
    }

    @Test
    public void testMapDataIncrease() throws IOException {
        Data newData = generalData
                .filter(record -> record.getAsJsonObject().get(BATCH_ID) == null)
                .map(record -> record.getAsJsonObject().get(EVENT_TYPE))
                .flatMap(object -> Stream.of(object, object));
        Assertions.assertEquals(newData.getLength(), 18);
    }

    @Test
    public void testShuffleData() throws IOException {
        Data newData = generalData
                .filter(record -> record.getAsJsonObject().get(BATCH_ID) != null)
                .map(record -> record.getAsJsonObject().get(EVENT_ID))
                .filter(record -> record.getAsString().contains("b"));
        Assertions.assertEquals(newData.getLength(), 12);
    }

    @Test
    public void testSiftLimitData() throws IOException {
        Data newData = generalData.sift(null, 2);
        Assertions.assertEquals(newData.getLength(), 2);
    }

    @Test
    public void testSiftSkipData() throws IOException {
        Data output1 = generalData.sift(null, 2);
        Data output2 = generalData.sift(2, 2);
        Assertions.assertNotEquals(output1, output2);
    }

    @Test
    public void testFindBy() {
        List<String> eventTypes = Arrays.asList("Checkpoint", "message");
        List<JsonElement> newData = generalData.findBy(EVENT_TYPE, eventTypes);
        List<JsonElement> found = new ArrayList<>();
        String expectedResult =
                "[\n" +
                        "{\n" +
                        "            \"batchId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4\",\n" +
                        "            \"eventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e\",\n" +
                        "            \"eventName\": \"Checkpoint\",\n" +
                        "            \"eventType\": \"Checkpoint\",\n" +
                        "            \"isBatched\": true,\n" +
                        "            \"parentEventId\": \"8bc787fe-d1b4-11eb-bae5-57b0c4472880\"\n" +
                        "},\n" +
                        "{\n" +
                        "            \"eventId\": \"8d44d930-d1b4-11eb-bae5-57b0c4472880\",\n" +
                        "            \"eventName\": \"Received 'ExecutionReport' response message\",\n" +
                        "            \"isBatched\": false,\n" +
                        "            \"eventType\": \"message\",\n" +
                        "            \"parentEventId\": \"8bc787fe-d1b4-11eb-bae5-57b0c4472880\"\n" +
                        "}\n" +
                "]";
        JsonArray events = JsonParser.parseString(expectedResult).getAsJsonArray();
        for (JsonElement event : events) {
            found.add(event.getAsJsonObject());
        }
        Assertions.assertEquals(newData, found);
    }

    @Test
    public void testWriteToFile() throws IOException {
        List<JsonElement> expectedList = new ArrayList<>();
        String expectedResult =
                "[\n" +
                        "{\n" +
                        "            \"batchId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4\",\n" +
                        "            \"eventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e\",\n" +
                        "            \"eventName\": \"Checkpoint\",\n" +
                        "            \"eventType\": \"Checkpoint\",\n" +
                        "            \"isBatched\": true,\n" +
                        "            \"parentEventId\": \"8bc787fe-d1b4-11eb-bae5-57b0c4472880\"\n" +
                        "},\n" +
                        "{\n" +
                        "            \"eventId\": \"8d44d930-d1b4-11eb-bae5-57b0c4472880\",\n" +
                        "            \"eventName\": \"Received 'ExecutionReport' response message\",\n" +
                        "            \"isBatched\": false,\n" +
                        "            \"eventType\": \"message\",\n" +
                        "            \"parentEventId\": \"8bc787fe-d1b4-11eb-bae5-57b0c4472880\"\n" +
                        "}\n" +
                        "]";
        JsonArray events = JsonParser.parseString(expectedResult).getAsJsonArray();
        for (JsonElement event : events) {
            expectedList.add(event.getAsJsonObject());
        }
        Data expectedData = new Data(expectedList);
        expectedData.writeToFile(tempFile.getAbsolutePath());
        String delimiter = "-".repeat(50);
        BufferedReader reader = Files.newBufferedReader(tempPath);
        String line = reader.readLine();
        StringBuilder actualResult = new StringBuilder("[");
        while (line != null) {
            if (line.equals(delimiter)) {
                actualResult.append(",");
            } else {
                actualResult.append(line);
            }
            line = reader.readLine();
        }
        actualResult.deleteCharAt(actualResult.lastIndexOf(",")).append("]");
        List<JsonElement> actualList = new ArrayList<>();
        JsonArray eventsFromFile = JsonParser.parseString(actualResult.toString()).getAsJsonArray();
        for (JsonElement event : eventsFromFile) {
            actualList.add(event.getAsJsonObject());
        }
        Data actualData = new Data(actualList);
        Assertions.assertEquals(expectedData, actualData);
    }

    @Test
    public void testCacheCommon() throws IOException {
        Data data = new Data(generalData.getData(), true);
        List<JsonElement> output1 = data.loadData(true);
        data.useCache(false);
        data.setData(new ArrayList<>());
        List<JsonElement> output2 = data.loadData(false);
        data.useCache(true);
        List<JsonElement> output3 = data.loadData(true);
        Assertions.assertEquals(output1, output3);
        Assertions.assertTrue(output2.isEmpty());
    }

    @Test
    public void testCacheForSource() throws IOException {
        Data data = new Data(generalData.getData(), true);
        Data data1 = data.filter(record -> record.getAsJsonObject().get("isBatched") != null &&
                record.getAsJsonObject().get("isBatched").getAsBoolean());
        Data data2 = data1.map(record -> {
            JsonObject newObject = new JsonObject();
            newObject.add("batch_status", record.getAsJsonObject().get("isBatched"));
            return newObject;
        });
        Data data3 = data1.flatMap(record -> Stream.of(record, record));
        Assertions.assertEquals(data2.getLastCache(), data3.getLastCache());
    }


}
