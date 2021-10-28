package com.exactpro.th2.dataservices;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;

import static com.exactpro.th2.dataservices.DataServicesUtils.*;

public class ConfTest {

    private static final LocalDateTime START_TIME = LocalDateTime.of(2021, 6, 15, 12, 44, 41, 692724000);
    private static final LocalDateTime END_TIME = LocalDateTime.of(2021, 6, 15, 15, 45, 49, 28579000);

    public static DataSource demoDataSource() {
        return new DataSource(String.format("http://%s:%s", DEMO_HOST, DEMO_PORT));
    }

    public static Data demoEventsFromDataSource(DataSource demoDataSource) throws IOException {
        Map<String, Object> props = new HashMap<>();
        props.put(START_TIMESTAMP, START_TIME);
        props.put(END_TIMESTAMP, END_TIME);
        props.put(METADATA_ONLY, false);
        //Returns 49 events
        //Failed =6
        return demoDataSource.getEventsFromDataProvider(
                false,
                props);
    }

    public static Data demoMessagesFromDataSource(DataSource demoDataSource) throws IOException {
        Map<String, Object> props = new HashMap<>();
        props.put(START_TIMESTAMP, START_TIME);
        props.put(END_TIMESTAMP, END_TIME);
        props.put(STREAM, Arrays.asList("th2-hand-demo", "demo-conn1"));
        //84 messages
        return demoDataSource.getMessagesFromDataProvider(
                false,
                props
        );
    }

    public static Data generalData() {
        List<JsonElement> result = new ArrayList<>();
        String data = "[\n" +
                "        {\n" +
                "            \"eventId\": \"84db48fc-d1b4-11eb-b0fb-199708acc7bc\",\n" +
                "            \"eventName\": \"[TS_1]Aggressive IOC vs two orders: second order's price is lower than first\",\n" +
                "            \"eventType\": \"\",\n" +
                "            \"isBatched\": false\n" +
                "        },\n" +
                "        {\n" +
                "            \"eventId\": \"88a3ee80-d1b4-11eb-b0fb-199708acc7bc\",\n" +
                "            \"eventName\": \"Case[TC_1.1]: Trader DEMO-CONN1 vs trader DEMO-CONN2 for instrument INSTR1\",\n" +
                "            \"eventType\": \"\",\n" +
                "            \"isBatched\": false,\n" +
                "            \"parentEventId\": \"84db48fc-d1b4-11eb-b0fb-199708acc7bc\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"eventId\": \"8bc787fe-d1b4-11eb-bae5-57b0c4472880\",\n" +
                "            \"eventName\": \"placeOrderFIX demo-conn1 - STEP1: Trader 'DEMO-CONN1' sends request to create passive Order.\",\n" +
                "            \"eventType\": \"placeOrderFIX\",\n" +
                "            \"isBatched\": false,\n" +
                "            \"parentEventId\": \"88a3ee80-d1b4-11eb-b0fb-199708acc7bc\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"batchId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4\",\n" +
                "            \"eventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"eventName\": \"Checkpoint\",\n" +
                "            \"eventType\": \"Checkpoint\",\n" +
                "            \"isBatched\": true,\n" +
                "            \"parentEventId\": \"8bc787fe-d1b4-11eb-bae5-57b0c4472880\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"batchId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4\",\n" +
                "            \"eventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c1114a4-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"eventName\": \"Checkpoint for session alias 'th2-hand-demo' direction 'FIRST' sequence '1623852603564709030'\",\n" +
                "            \"eventType\": \"Checkpoint for session\",\n" +
                "            \"isBatched\": true,\n" +
                "            \"parentEventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"batchId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4\",\n" +
                "            \"eventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c1114a5-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"eventName\": \"Checkpoint for session alias 'demo-conn1' direction 'SECOND' sequence '1624005455622140289'\",\n" +
                "            \"eventType\": \"Checkpoint for session\",\n" +
                "            \"parentEventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"batchId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4\",\n" +
                "            \"eventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c1114a6-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"eventName\": \"Checkpoint for session alias 'demo-dc1' direction 'SECOND' sequence '1624005475721015014'\",\n" +
                "            \"eventType\": \"Checkpoint for session\",\n" +
                "            \"isBatched\": true,\n" +
                "            \"parentEventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"batchId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4\",\n" +
                "            \"eventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c1114a7-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"eventName\": \"Checkpoint for session alias 'demo-dc1' direction 'FIRST' sequence '1624005475720919499'\",\n" +
                "            \"eventType\": \"Checkpoint for session\",\n" +
                "            \"isBatched\": true,\n" +
                "            \"parentEventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"batchId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4\",\n" +
                "            \"eventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c1114a8-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"eventName\": \"Checkpoint for session alias 'demo-conn2' direction 'FIRST' sequence '1624005448022245399'\",\n" +
                "            \"eventType\": \"Checkpoint for session\",\n" +
                "            \"isBatched\": true,\n" +
                "            \"parentEventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"batchId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4\",\n" +
                "            \"eventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c1114a9-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"eventName\": \"Checkpoint for session alias 'demo-conn2' direction 'SECOND' sequence '1624005448022426113'\",\n" +
                "            \"eventType\": \"Checkpoint for session\",\n" +
                "            \"isBatched\": true,\n" +
                "            \"parentEventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"batchId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4\",\n" +
                "            \"eventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c1114aa-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"eventName\": \"Checkpoint for session alias 'demo-dc2' direction 'SECOND' sequence '1624005466840347015'\",\n" +
                "            \"eventType\": \"Checkpoint for session\",\n" +
                "            \"isBatched\": true,\n" +
                "            \"parentEventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"batchId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4\",\n" +
                "            \"eventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c1114ab-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"eventName\": \"Checkpoint for session alias 'demo-dc2' direction 'FIRST' sequence '1624005466840263372'\",\n" +
                "            \"eventType\": \"Checkpoint for session\",\n" +
                "            \"isBatched\": true,\n" +
                "            \"parentEventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"batchId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4\",\n" +
                "            \"eventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c1114ac-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"eventName\": \"Checkpoint for session alias 'demo-conn1' direction 'FIRST' sequence '1624005455622011522'\",\n" +
                "            \"eventType\": \"Checkpoint for session\",\n" +
                "            \"isBatched\": true,\n" +
                "            \"parentEventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"batchId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4\",\n" +
                "            \"eventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c1114ad-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"eventName\": \"Checkpoint for session alias 'demo-log' direction 'FIRST' sequence '1624029363623063053'\",\n" +
                "            \"eventType\": \"Checkpoint for session\",\n" +
                "            \"isBatched\": true,\n" +
                "            \"parentEventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"eventId\": \"8c3fec4f-d1b4-11eb-bae5-57b0c4472880\",\n" +
                "            \"eventName\": \"Send 'NewOrderSingle' message to connectivity\",\n" +
                "            \"eventType\": \"Outgoing message\",\n" +
                "            \"isBatched\": false,\n" +
                "            \"parentEventId\": \"8bc787fe-d1b4-11eb-bae5-57b0c4472880\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"eventId\": \"8c44806c-d1b4-11eb-8e55-d3a76285d588\",\n" +
                "            \"eventName\": \"Send 'NewOrderSingle' message\",\n" +
                "            \"eventType\": \"Outgoing message\",\n" +
                "            \"isBatched\": false,\n" +
                "            \"parentEventId\": \"8bc787fe-d1b4-11eb-bae5-57b0c4472880\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"batchId\": \"654c2724-5202-460b-8e6c-a7ee9fb02ddf\",\n" +
                "            \"eventId\": \"654c2724-5202-460b-8e6c-a7ee9fb02ddf:8ca20288-d1b4-11eb-986f-1e8d42132387\",\n" +
                "            \"eventName\": \"Remove 'NewOrderSingle' " +
                "            id='demo-conn1:SECOND:1624005455622135205' " +
                "            Hash='7009491514226292581' Group='NOS_CONN' " +
                "            Hash['SecondaryClOrdID': 11111, 'SecurityID': INSTR1]\",\n" +
                "            \"isBatched\": true,\n" +
                "            \"eventType\": \"\",\n" +
                "            \"parentEventId\": \"a3779b94-d051-11eb-986f-1e8d42132387\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"eventId\": \"8ceb47f6-d1b4-11eb-a9ed-ffb57363e013\",\n" +
                "            \"eventName\": \"Send 'ExecutionReport' message\",\n" +
                "            \"isBatched\": false,\n" +
                "            \"eventType\": \"Send message\",\n" +
                "            \"parentEventId\": \"845d70d2-9c68-11eb-8598-691ebd7f413d\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"eventId\": \"8ced1c93-d1b4-11eb-a9f4-b12655548efc\",\n" +
                "            \"eventName\": \"Send 'ExecutionReport' message\",\n" +
                "            \"isBatched\": false,\n" +
                "            \"eventType\": \"Send message\",\n" +
                "            \"parentEventId\": \"845d70d2-9c68-11eb-8598-691ebd7f413d\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"eventId\": \"8d44d930-d1b4-11eb-bae5-57b0c4472880\",\n" +
                "            \"eventName\": \"Received 'ExecutionReport' response message\",\n" +
                "            \"isBatched\": false,\n" +
                "            \"eventType\": \"message\",\n" +
                "            \"parentEventId\": \"8bc787fe-d1b4-11eb-bae5-57b0c4472880\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"eventId\": \"8d6e0c9e-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"eventName\": \"Check sequence rule SessionKey{sessionAlias='demo-conn1', " +
                "            direction=FIRST} - STEP2: Trader 'DEMO-CONN1' receives " +
                "            Execution Report. The order stands on book in status NEW\",\n" +
                "            \"isBatched\": false,\n" +
                "            \"eventType\": \"Checkpoint for session\",\n" +
                "            \"parentEventId\": \"88a3ee80-d1b4-11eb-b0fb-199708acc7bc\"\n" +
                "        }\n" +
                "    ]";
        JsonArray events = JsonParser.parseString(data).getAsJsonArray();
        for (JsonElement event : events) {
            result.add(event.getAsJsonObject());
        }
        return new Data(result);
    }

}
