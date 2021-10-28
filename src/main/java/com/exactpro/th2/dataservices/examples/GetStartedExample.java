package com.exactpro.th2.dataservices.examples;

import com.exactpro.th2.dataservices.Data;
import com.exactpro.th2.dataservices.DataSource;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.exactpro.th2.dataservices.DataServicesUtils.*;

public class GetStartedExample {

    public static final LocalDateTime START_TIME = LocalDateTime.of(2021, 6, 17, 12, 44, 41, 692724000);

    public static final LocalDateTime END_TIME = LocalDateTime.of(2021, 6, 17, 15, 45, 49, 28579000);

    public static void main(String[] args) throws IOException {
        //[1] Create DataSource object to connect to rpt-data-provider.
        DataSource dataSource = new DataSource(String.format("http://%s:%s", DEMO_HOST, DEMO_PORT));

        //[2] Get events from START_TIME to END_TIME.
        Map<String, Object> props = new HashMap<>();
        props.put(START_TIMESTAMP, START_TIME);
        props.put(END_TIMESTAMP, END_TIME);
        props.put(METADATA_ONLY, false);
        props.put(ATTACHED_MESSAGES, true);
        Data events = dataSource.getEventsFromDataProvider(
                false,
                props
        );

        //[3] Work with your Data object.
        // [3.1] Filter.
        //Filter events with empty body.
        Data filteredEvents = events.filter(x -> x.getAsJsonObject().get(BODY) != null &&
                !x.getAsJsonObject().get(BODY).toString().equals("[]"));
        System.out.println("Filtered events: \n" + filteredEvents);

        //[3.2] Map.
        Function<JsonElement, JsonObject> transformFunction = record -> {
            JsonObject newObject = new JsonObject();
            newObject.add(EVENT_NAME, record.getAsJsonObject().get(EVENT_NAME));
            newObject.add(SUCCESSFUL, record.getAsJsonObject().get(SUCCESSFUL));
            return newObject;
        };

        Data filteredAndMappedEvents = filteredEvents.map(transformFunction);
        System.out.println("Filtered and mapped events: \n" + filteredAndMappedEvents);

        //[3.3] Data pipeline.
        //Instead of doing data transformations step by step you can do it in one line.
        Data filteredAndMappedEventsByPipeline = events
                .filter(x -> x.getAsJsonObject().get(BODY) != null &&
                        x.getAsJsonObject().get(BODY).toString().equals("[]"))
                .map(transformFunction);
        System.out.println("Filtered and mapped events by pipeline: \n" + filteredAndMappedEventsByPipeline);


        //Content of these two Data objects should be equal.
        assert filteredAndMappedEvents.equals(filteredAndMappedEventsByPipeline);

        //[3.4] Sift. Skip the first few items or limit them.
        Data eventsFromElevenToEnd = events.sift(10, null);
        Data onlyFirstTenEvents = events.sift(null, 10);
        System.out.println("Events from eleven to end: \n" + eventsFromElevenToEnd);
        System.out.println("Only first ten events: \n" + onlyFirstTenEvents);

        //[3.5] Walk through data.
        //Do something with event
        events.forEach(System.out::println);

        //[3.6] Get number of the elements in the Data object.
        long numberOfEvents = events.getLength();
        System.out.println("Events length: " + numberOfEvents);

        //[3.7] Get event/message by id.
        String desiredEvent = "9ce8a2ff-d600-4366-9aba-2082cfc69901:ef1d722e-cf5e-11eb-bcd0-ced60009573f";
        List<String> desiredEvents = Arrays.asList(
                "deea079b-4235-4421-abf6-6a3ac1d04c76:ef1d3a20-cf5e-11eb-bcd0-ced60009573f",
                "a34e3cb4-c635-4a90-8f42-37dd984209cb:ef1c5cea-cf5e-11eb-bcd0-ced60009573f");

        String desiredMessage = "demo-conn1:first:1619506157132265837";
        List<String> desiredMessages = Arrays.asList(
                "demo-conn1:first:1619506157132265836",
                "demo-conn1:first:1619506157132265833");


        System.out.println("Found event: " + dataSource.findEventsByIdFromDataProvider(desiredEvent));  // Returns 1 event.
        System.out.println("Found events: " + dataSource.findEventsByIdFromDataProvider(desiredEvents.get(0), desiredEvents.get(1)));  // Returns 2 events.

        System.out.println("Found message: " + dataSource.findMessagesByIdFromDataProvider(desiredMessage)); // Returns 1 message.
        System.out.println("Found messages: " + dataSource.findMessagesByIdFromDataProvider(desiredMessages.get(0), desiredMessages.get(1))); // Returns 2 messages.

    }
}
