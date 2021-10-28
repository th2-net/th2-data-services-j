package com.exactpro.th2.dataservices;

import com.google.gson.*;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static com.exactpro.th2.dataservices.DataServicesUtils.*;

public class TestDataSourceLocal {

    private static DataSource demoDataSource;

    private static Path tempPath;
    private static File tempFile;

    private static Path tempPath1;
    private static File tempFile1;

    @TempDir
    static Path tempDir;

    @BeforeAll
    public static void setUp() {
        demoDataSource = ConfTest.demoDataSource();
        tempPath = tempDir.resolve("test.txt");
        tempFile = tempPath.toFile();
        tempPath1 = tempDir.resolve("test1.txt");
        tempFile1 = tempPath1.toFile();
    }

    @Test
    public void testFindEventsByIdFromDataProvider() {
        String expEvent1 = "{\n" +
                "        \"attachedMessageIds\": [],\n" +
                "        \"batchId\": null,\n" +
                "        \"body\": {},\n" +
                "        \"endTimestamp\": {\"epochSecond\": 1624185888, \"nano\": 169710000},\n" +
                "        \"eventId\": \"88a3ee80-d1b4-11eb-b0fb-199708acc7bc\",\n" +
                "        \"eventName\": \"Case[TC_1.1]: Trader DEMO-CONN1 vs trader DEMO-CONN2 for instrument INSTR1\",\n" +
                "        \"eventType\": \"\",\n" +
                "        \"isBatched\": false,\n" +
                "        \"parentEventId\": \"84db48fc-d1b4-11eb-b0fb-199708acc7bc\",\n" +
                "        \"startTimestamp\": {\"epochSecond\": 1624185888, \"nano\": 169672000},\n" +
                "        \"successful\": true,\n" +
                "        \"type\": \"event\"\n" +
                "    }";
        JsonObject expectedEvent1 = JsonParser.parseString(expEvent1).getAsJsonObject();
        String expEvent2 = "{\n" +
                "            \"attachedMessageIds\": [\n" +
                "                \"demo-conn1:first:1624005455622011522\",\n" +
                "                \"demo-conn1:second:1624005455622140289\",\n" +
                "                \"demo-conn2:first:1624005448022245399\",\n" +
                "                \"demo-conn2:second:1624005448022426113\",\n" +
                "                \"demo-dc1:first:1624005475720919499\",\n" +
                "                \"demo-dc1:second:1624005475721015014\",\n" +
                "                \"demo-dc2:first:1624005466840263372\",\n" +
                "                \"demo-dc2:second:1624005466840347015\",\n" +
                "                \"demo-log:first:1624029363623063053\",\n" +
                "                \"th2-hand-demo:first:1623852603564709030\"\n" +
                "            ],\n" +
                "            \"batchId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4\",\n" +
                "            \"body\": [\n" +
                "                {\n" +
                "                    \"data\": \"Checkpoint id '8c037f50-d1b4-11eb-ba78-1981398e00bd'\",\n" +
                "                    \"type\": \"message\"\n" +
                "                }\n" +
                "            ],\n" +
                "            \"endTimestamp\": {\"epochSecond\": 1624185893, \"nano\": 830158000},\n" +
                "            \"eventId\": \"6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"eventName\": \"Checkpoint\",\n" +
                "            \"eventType\": \"Checkpoint\",\n" +
                "            \"isBatched\": True,\n" +
                "            \"parentEventId\": \"8bc787fe-d1b4-11eb-bae5-57b0c4472880\",\n" +
                "            \"startTimestamp\": {\"epochSecond\": 1624185893, \"nano\": 828017000},\n" +
                "            \"successful\": True,\n" +
                "            \"type\": \"event\"\n" +
                "        }";
        JsonObject expectedEvent2 = JsonParser.parseString(expEvent2).getAsJsonObject();
        Set<JsonObject> expectedEvents = new HashSet<>();
        expectedEvents.add(expectedEvent1);
        expectedEvents.add(expectedEvent2);

        Set<JsonObject> actualEvent = demoDataSource.findEventsByIdFromDataProvider("88a3ee80-d1b4-11eb-b0fb-199708acc7bc");
        Set<JsonObject> actualEvents = demoDataSource.findEventsByIdFromDataProvider("88a3ee80-d1b4-11eb-b0fb-199708acc7bc",
                "6e3be13f-cab7-4653-8cb9-6e74fd95ade4:8c035903-d1b4-11eb-9278-591e568ad66e");

        for (JsonObject event : actualEvents) {
            JsonArray attachedMessageIds = event.getAsJsonArray(ATTACHED_MESSAGE_ID);
            List<String> messagesIds = new ArrayList<>();
            for (JsonElement msg : attachedMessageIds) {
                messagesIds.add(msg.getAsString());
            }
            Collections.sort(messagesIds);
            JsonArray sorted = new JsonArray();
            messagesIds.forEach(sorted::add);
            event.add(ATTACHED_MESSAGE_ID, sorted);
        }

        Assertions.assertEquals(expectedEvent1, actualEvent.stream().findFirst().orElse(null));
        Assertions.assertEquals(expectedEvents, actualEvents);
        Assertions.assertEquals(actualEvents.size(), 2);
    }

    @Test
    public void testFindMessagesByIdFromDataProvider() {
        String expMessage1 = "{\n" +
                "        \"attachedEventIds\": [\n" +
                "            \"8ea15c95-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"8ea15c9a-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"8d7fbfe9-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"8d7fbfe4-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"8c035903-d1b4-11eb-9278-591e568ad66e\",\n" +
                "            \"8c1114a8-d1b4-11eb-9278-591e568ad66e\"\n" +
                "        ],\n" +
                "        \"type\": \"message\",\n" +
                "        \"timestamp\": {\"nano\": 123000000, \"epochSecond\": 1624185872},\n" +
                "        \"direction\": \"IN\",\n" +
                "        \"sessionId\": \"demo-conn2\",\n" +
                "        \"messageType\": \"Heartbeat\",\n" +
                "        \"body\": {\n" +
                "            \"metadata\": {\n" +
                "                \"id\": {\n" +
                "                    \"connectionId\": {\"sessionAlias\": \"demo-conn2\"},\n" +
                "                    \"sequence\": \"1624005448022245399\",\n" +
                "                    \"subsequence\": [1]\n" +
                "                },\n" +
                "                \"timestamp\": \"2021-06-20T10:44:32.123Z\",\n" +
                "                \"messageType\": \"Heartbeat\"\n" +
                "            },\n" +
                "            \"fields\": {\n" +
                "                \"trailer\": {\"messageValue\": {\"fields\": {\"CheckSum\": {\"simpleValue\": \"073\"}}}},\n" +
                "                \"header\": {\n" +
                "                    \"messageValue\": {\n" +
                "                        \"fields\": {\n" +
                "                            \"BeginString\": {\"simpleValue\": \"FIXT.1.1\"},\n" +
                "                            \"SenderCompID\": {\"simpleValue\": \"FGW\"},\n" +
                "                            \"SendingTime\": {\"simpleValue\": \"2021-06-20T10:44:32.122\"},\n" +
                "                            \"TargetCompID\": {\"simpleValue\": \"DEMO-CONN2\"},\n" +
                "                            \"MsgType\": {\"simpleValue\": \"0\"},\n" +
                "                            \"MsgSeqNum\": {\"simpleValue\": \"1290\"},\n" +
                "                            \"BodyLength\": {\"simpleValue\": \"59\"}\n" +
                "                        }\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        },\n" +
                "        \"bodyBase64\": \"OD1GSVhULjEuMQE5PTU5ATM1PTABMzQ9MTI5MAE0OT1GR1cBNTI9MjAyMTA2MjAtMTA6NDQ6MzIuMTIyATU2PURFTU8tQ09OTjIBMTA9MDczAQ==\",\n" +
                "        \"messageId\": \"demo-conn2:first:1624005448022245399\"\n" +
                "    }";
        JsonObject expectedMessage1 = JsonParser.parseString(expMessage1).getAsJsonObject();
        Set<JsonObject> expectedMessages = new HashSet<>();
        expectedMessages.add(expectedMessage1);
        String expMessage2 = "{\n" +
                "            \"attachedEventIds\": [\n" +
                "                \"8ff68c4f-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"a6401d79-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"92a13912-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"8f36e5b4-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"e2be6278-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"935ee3d3-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"9d829081-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"e1ece1a1-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"40fd753d-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"cde689e4-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"ddf828a2-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"e3559979-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"3f9d9705-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"405acc8c-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"981a705c-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"8ea15c95-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"d93262e9-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"464fe98c-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"9a0f3b84-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"534490a8-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"cf34188b-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"4e970df3-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"d048e3fc-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"908a8f00-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"93f77b43-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"de96b299-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"e6b5dba0-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"d47353fa-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"98b3cb12-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"cde662ca-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"dd26f5d7-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"8a1fa4c0-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"d86b4248-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"e3559983-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"a2d776ec-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"d86b6962-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"534490b2-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"9e3a95e3-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"9554e885-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"3b261873-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"4fe475a5-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"a2d776e2-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"935ee3dd-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"9d7968bc-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"3a08735c-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"981a7052-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"45ba392b-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"e6b5db96-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"3c38e818-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"9e3a95ed-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"d9ca3634-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"9ed3c8be-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"9554e88f-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"93ffdede-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"d50a3ce5-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"94b721d4-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"405acc96-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"908a67e6-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"9ed3c8b4-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"94b721de-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"e2be6282-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"a18be3f0-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"93ffb7c4-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"4b1b2d85-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"3f9d96fb-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"8ea15c9f-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"d0490b16-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"de96b2a3-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"d3a79f79-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"4fe4759b-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"464fe996-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"9c59a0da-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"8d7fbfee-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"8a1fa4b6-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"4f443dea-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"a2e77c5b-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"894b8bcf-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"9d82908b-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"8b2c59e1-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"ddf80188-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"9d7941a2-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"d4737b14-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"49da7003-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"44fbf334-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"4a804cfa-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"3b261869-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"8f36e5be-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"4e96e6d9-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"93f77b4d-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"45ba3935-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"9e1337d2-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"d93262f3-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"8ff68c45-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"9e1337dc-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"9c5979c0-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"1e67d08a-d048-11eb-986f-1e8d42132387\",\n" +
                "                \"d3a79f83-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"98b3cb1c-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"9a0f629e-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"40fd7547-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"97683741-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"dd26f5e1-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"e1ece197-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"4b1b2d7b-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"3a089a76-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"4a807414-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"cf341895-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"99793d0d-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"a18c0b0a-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"a2445e9b-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"92a1391c-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"8c1114ad-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"8b2c59d7-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"8d7fbfe4-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"a2e77c51-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"44fbf32a-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"4f446504-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"894b8bc5-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"3c38e80e-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"8c035903-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"9768374b-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"49da6ff9-d052-11eb-9278-591e568ad66e\",\n" +
                "                \"997915f3-d051-11eb-9278-591e568ad66e\",\n" +
                "                \"98b41853-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"98b4185d-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"d9ca362a-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"a6401d6f-d1b4-11eb-9278-591e568ad66e\",\n" +
                "                \"d50a3cdb-d295-11eb-9278-591e568ad66e\",\n" +
                "                \"a2445e91-d1b4-11eb-9278-591e568ad66e\"\n" +
                "            ],\n" +
                "            \"type\": \"message\",\n" +
                "            \"timestamp\": {\"nano\": 820976000, \"epochSecond\": 1624029370},\n" +
                "            \"direction\": \"IN\",\n" +
                "            \"sessionId\": \"demo-log\",\n" +
                "            \"messageType\": \"NewOrderSingle\",\n" +
                "            \"body\": {\n" +
                "                \"metadata\": {\n" +
                "                    \"id\": {\n" +
                "                        \"connectionId\": {\"sessionAlias\": \"demo-log\"},\n" +
                "                        \"sequence\": \"1624029363623063053\",\n" +
                "                        \"subsequence\": [1]\n" +
                "                    },\n" +
                "                    \"timestamp\": \"2021-06-18T15:16:10.820976Z\",\n" +
                "                    \"messageType\": \"NewOrderSingle\"\n" +
                "                },\n" +
                "                \"fields\": {\n" +
                "                    \"OrderQty\": {\"simpleValue\": \"100\"},\n" +
                "                    \"OrdType\": {\"simpleValue\": \"2\"},\n" +
                "                    \"ClOrdID\": {\"simpleValue\": \"1687434\"},\n" +
                "                    \"SecurityIDSource\": {\"simpleValue\": \"8\"},\n" +
                "                    \"OrderCapacity\": {\"simpleValue\": \"A\"},\n" +
                "                    \"TransactTime\": {\"simpleValue\": \"2020-11-24T13:58:26.270\"},\n" +
                "                    \"SecondaryClOrdID\": {\"simpleValue\": \"33333\"},\n" +
                "                    \"AccountType\": {\"simpleValue\": \"1\"},\n" +
                "                    \"trailer\": {\"messageValue\": {\"fields\": {\"CheckSum\": {\"simpleValue\": \"209\"}}}},\n" +
                "                    \"Side\": {\"simpleValue\": \"2\"},\n" +
                "                    \"Price\": {\"simpleValue\": \"34\"},\n" +
                "                    \"TimeInForce\": {\"simpleValue\": \"3\"},\n" +
                "                    \"TradingParty\": {\n" +
                "                        \"messageValue\": {\n" +
                "                            \"fields\": {\n" +
                "                                \"NoPartyIDs\": {\n" +
                "                                    \"listValue\": {\n" +
                "                                        \"values\": [\n" +
                "                                            {\n" +
                "                                                \"messageValue\": {\n" +
                "                                                    \"fields\": {\n" +
                "                                                        \"PartyRole\": {\"simpleValue\": \"76\"},\n" +
                "                                                        \"PartyID\": {\"simpleValue\": \"DEMO-CONN2\"},\n" +
                "                                                        \"PartyIDSource\": {\"simpleValue\": \"D\"}\n" +
                "                                                    }\n" +
                "                                                }\n" +
                "                                            },\n" +
                "                                            {\n" +
                "                                                \"messageValue\": {\n" +
                "                                                    \"fields\": {\n" +
                "                                                        \"PartyRole\": {\"simpleValue\": \"3\"},\n" +
                "                                                        \"PartyID\": {\"simpleValue\": \"0\"},\n" +
                "                                                        \"PartyIDSource\": {\"simpleValue\": \"P\"}\n" +
                "                                                    }\n" +
                "                                                }\n" +
                "                                            },\n" +
                "                                            {\n" +
                "                                                \"messageValue\": {\n" +
                "                                                    \"fields\": {\n" +
                "                                                        \"PartyRole\": {\"simpleValue\": \"122\"},\n" +
                "                                                        \"PartyID\": {\"simpleValue\": \"0\"},\n" +
                "                                                        \"PartyIDSource\": {\"simpleValue\": \"P\"}\n" +
                "                                                    }\n" +
                "                                                }\n" +
                "                                            },\n" +
                "                                            {\n" +
                "                                                \"messageValue\": {\n" +
                "                                                    \"fields\": {\n" +
                "                                                        \"PartyRole\": {\"simpleValue\": \"12\"},\n" +
                "                                                        \"PartyID\": {\"simpleValue\": \"3\"},\n" +
                "                                                        \"PartyIDSource\": {\"simpleValue\": \"P\"}\n" +
                "                                                    }\n" +
                "                                                }\n" +
                "                                            }\n" +
                "                                        ]\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        }\n" +
                "                    },\n" +
                "                    \"SecurityID\": {\"simpleValue\": \"INSTR2\"},\n" +
                "                    \"header\": {\n" +
                "                        \"messageValue\": {\n" +
                "                            \"fields\": {\n" +
                "                                \"BeginString\": {\"simpleValue\": \"FIXT.1.1\"},\n" +
                "                                \"SenderCompID\": {\"simpleValue\": \"DEMO-CONN2\"},\n" +
                "                                \"SendingTime\": {\"simpleValue\": \"2020-11-24T10:58:26.317\"},\n" +
                "                                \"TargetCompID\": {\"simpleValue\": \"FGW\"},\n" +
                "                                \"MsgType\": {\"simpleValue\": \"D\"},\n" +
                "                                \"MsgSeqNum\": {\"simpleValue\": \"443\"},\n" +
                "                                \"BodyLength\": {\"simpleValue\": \"250\"}\n" +
                "                            }\n" +
                "                        }\n" +
                "                    }\n" +
                "                }\n" +
                "            },\n" +
                "            \"bodyBase64\": \"OD1GSVhULjEuMQE5PTI1MAEzNT1EATM0PTQ0MwE0OT1ERU1PLUNPTk4yATUyPTIwMjAxMTI0LTEwOjU4OjI2LjMxNwE1Nj1GR1cBMTE9MTY4NzQzNAEyMj04ATM4PTEwMAE0MD0yATQ0PTM0ATQ4PUlOU1RSMgE1ND0yATU5PTMBNjA9MjAyMDExMjQtMTM6NTg6MjYuMjcwATUyNj0zMzMzMwE1Mjg9QQE1ODE9MQE0NTM9NAE0NDg9REVNTy1DT05OMgE0NDc9RAE0NTI9NzYBNDQ4PTABNDQ3PVABNDUyPTMBNDQ4PTABNDQ3PVABNDUyPTEyMgE0NDg9MwE0NDc9UAE0NTI9MTIBMTA9MjA5AQ==\",\n" +
                "            \"messageId\": \"demo-log:first:1624029363623063053\"\n" +
                "        }";
        JsonObject expectedMessage2 = JsonParser.parseString(expMessage2).getAsJsonObject();
        expectedMessages.add(expectedMessage2);
        Set<JsonObject> actualMessage = demoDataSource.findMessagesByIdFromDataProvider("demo-conn2:first:1624005448022245399");
        Set<JsonObject> actualMessages = demoDataSource.findMessagesByIdFromDataProvider("demo-conn2:first:1624005448022245399",
                "demo-log:first:1624029363623063053");

        Assertions.assertEquals(expectedMessage1, actualMessage.stream().findFirst().orElse(null));
        Assertions.assertEquals(expectedMessages, actualMessages);
        Assertions.assertEquals(actualMessages.size(), 2);
    }

    @Test
    public void testFindMessageByIdFromDataProviderWithError() {
        ValueError ex = Assertions.assertThrows(ValueError.class,
                () -> demoDataSource.findMessagesByIdFromDataProvider("demo-conn_not_exist:first:1624005448022245399"));
        Assertions.assertTrue(ex.getMessage().contains("Sorry, but the answer rpt-data-provider doesn't match the json format."));
    }

    @Test
    public void testGetEventsFromDataProviderWithError() {
        Map<String, Object> params = new HashMap<>();
        params.put(START_TIMESTAMP, "test");
        params.put(END_TIMESTAMP, "test");
        HttpError ex = Assertions.assertThrows(HttpError.class, () ->
                demoDataSource.getEventsFromDataProvider(false, params));
        Assertions.assertTrue(ex.getMessage().contains(
                "{\"exceptionName\":\"java.lang.NumberFormatException\",\"exceptionCause\":\"For input string: \\\"test\\\"\"}"
        ));
    }

    @Test
    public void testGetMessagesFromDataProviderWithError() {
        Map<String, Object> params = new HashMap<>();
        params.put(START_TIMESTAMP, "test");
        params.put(END_TIMESTAMP, "test");
        params.put(STREAM, "test");
        HttpError ex = Assertions.assertThrows(HttpError.class, () ->
                demoDataSource.getMessagesFromDataProvider(false, params));
        Assertions.assertTrue(ex.getMessage().contains(
                "{\"exceptionName\":\"java.lang.NumberFormatException\",\"exceptionCause\":\"For input string: \\\"test\\\"\"}"
        ));
    }

    @Test
    public void testCheckUrlForDataSource() {
        HttpError ex = Assertions.assertThrows(HttpError.class, () ->
                new DataSource("http://test_test:8080/"));
        Assertions.assertTrue(ex.getMessage().contains("Unable to connect to host"));
    }

    @Test
    public void testReadCsv() throws CsvValidationException, IOException {
        CSVWriter writer = new CSVWriter(new FileWriter(tempFile.getAbsolutePath()));
        List<String[]> allLines = new LinkedList<>();
        allLines.add("x,y,z".split(","));
        allLines.add("1,2,www".split(","));
        allLines.add("3,4,yyy".split(","));
        writer.writeAll(allLines);
        writer.close();
        writer = new CSVWriter(new FileWriter(tempFile1.getAbsolutePath()));
        allLines.clear();
        allLines.add("a,b,c".split(","));
        allLines.add("12,32,45".split(","));
        allLines.add("10,20,30".split(","));
        writer.writeAll(allLines);
        writer.close();
        String expectedData = "------------- Printed first 5 records -------------\n" +
                "{\n" +
                "  \"x\": \"1\",\n" +
                "  \"y\": \"2\",\n" +
                "  \"z\": \"www\"\n" +
                "}\n" +
                "{\n" +
                "  \"x\": \"3\",\n" +
                "  \"y\": \"4\",\n" +
                "  \"z\": \"yyy\"\n" +
                "}\n" +
                "{\n" +
                "  \"a\": \"12\",\n" +
                "  \"b\": \"32\",\n" +
                "  \"c\": \"45\"\n" +
                "}\n" +
                "{\n" +
                "  \"a\": \"10\",\n" +
                "  \"b\": \"20\",\n" +
                "  \"c\": \"30\"\n" +
                "}\n";
        Assertions.assertEquals(DataSource.readCsvFile(tempFile.getAbsolutePath(),
                tempFile1.getAbsolutePath()).toString(), expectedData);
    }
}
