package com.exactpro.th2.dataservices;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.exactpro.th2.dataservices.DataServicesUtils.SUCCESSFUL;

public class TestDataLocal {

    private static Data demoEvents;
    private static Data demoMessages;

    @BeforeAll
    public static void setUp() throws IOException {
        demoEvents = ConfTest.demoEventsFromDataSource(ConfTest.demoDataSource());
        demoMessages = ConfTest.demoMessagesFromDataSource(ConfTest.demoDataSource());
    }

    @Test
    public void testLenEvents() {
        Assertions.assertEquals(demoEvents.getLength(), 49);
    }

    @Test
    public void testLenMessages() {
        Assertions.assertEquals(demoMessages.getLength(), 84);
    }

    @Test
    public void testFilterData() throws IOException {
        Data newData = demoEvents.filter(r -> !r.getAsJsonObject().get(SUCCESSFUL).getAsBoolean());
        Assertions.assertEquals(newData.getLength(), 6);
    }
}
