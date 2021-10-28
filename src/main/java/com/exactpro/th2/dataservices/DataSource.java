package com.exactpro.th2.dataservices;

import com.google.gson.*;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.sse.SseEventSource;

import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.exactpro.th2.dataservices.DataServicesUtils.*;

public class DataSource {

    private String url;
    private HttpClient client;

    public DataSource(String url) {
        this.url = url;
        checkConnect();
    }

    public void checkConnect() {
        try {
            client = HttpClient.newHttpClient();
            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(5))
                    .build();
                client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
                        .join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof ConnectException) {
                throw new HttpError("Unable to connect to host " + url, e);
            }
            throw e;
        } catch (IllegalArgumentException e) {
            throw new HttpError("Unable to connect to host " + url, e);
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        if (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }
        this.url = url;
    }

    public List<JsonElement> sseRequestToDataProvider(Map<String, Object> props) {
        Object route = props.get(ROUTE);
        if (route == null) {
            throw new ValueError("Route is required field. Please fill it.");
        }
        if (props.get(START_TIMESTAMP) != null && props.get(START_TIMESTAMP) instanceof LocalDateTime) {
            LocalDateTime start = (LocalDateTime) props.get(START_TIMESTAMP);
            //unix timestamp in milliseconds
            long startMilliseconds = convertToMilliseconds(start);
            props.put(START_TIMESTAMP, startMilliseconds);
        }

        if (props.get(END_TIMESTAMP) != null && props.get(END_TIMESTAMP) instanceof LocalDateTime) {
            LocalDateTime end = (LocalDateTime) props.get(END_TIMESTAMP);
            long endMilliseconds = convertToMilliseconds(end);
            props.put(END_TIMESTAMP, endMilliseconds);
        }
        String url = this.url + route;
        url = urlEncode(url, props);
        return executeSseRequest(url);
    }

    public Data getEventsFromDataProvider(boolean cache, Map<String, Object> props) throws IOException {
        if (props.get(START_TIMESTAMP) == null && props.get(RESUME_FROM_ID) == null) {
            throw new ValueError(
                    String.format(
                            "'%s' or '%s' must not be null for route /search/sse/events. " +
                                    "Please note it. " +
                                    "More information on request here: https://github.com/th2-net/th2-rpt-data-provider",
                            START_TIMESTAMP,
                            RESUME_FROM_ID
                    )
            );
        }
        if (props.get(START_TIMESTAMP) instanceof LocalDateTime) {
            LocalDateTime start = (LocalDateTime) props.get(START_TIMESTAMP);
            //unix timestamp in milliseconds
            long startMilliseconds = convertToMilliseconds(start);
            props.put(START_TIMESTAMP, startMilliseconds);
        }

        if (props.get(END_TIMESTAMP) != null && props.get(END_TIMESTAMP) instanceof LocalDateTime) {
            LocalDateTime end = (LocalDateTime) props.get(END_TIMESTAMP);
            long endMilliseconds = convertToMilliseconds(end);
            props.put(END_TIMESTAMP, endMilliseconds);
        }

        String url = this.url + "/search/sse/events";
        url = urlEncode(url, props);
        return new Data(executeSseRequest(url), cache);
    }

    public Data getMessagesFromDataProvider(boolean cache, Map<String, Object> props) throws IOException{
        if (props.get(START_TIMESTAMP) == null && props.get(RESUME_FROM_ID) == null) {
            throw new ValueError(
                    String.format(
                            "'%s' or '%s' must not be null for route /search/sse/messages. " +
                                    "Please note it. " +
                                    "More information on request here: https://github.com/th2-net/th2-rpt-data-provider",
                            START_TIMESTAMP,
                            RESUME_FROM_ID
                    )
            );
        }
        if (props.get(STREAM) == null) {
            throw new ValueError(
                    String.format(
                            "'%s' is required field. Please note it. " +
                                    "More information on request here: https://github.com/th2-net/th2-rpt-data-provider",
                            STREAM
                    )
            );
        }

        if (props.get(START_TIMESTAMP) instanceof LocalDateTime) {
            LocalDateTime start = (LocalDateTime) props.get(START_TIMESTAMP);
            //unix timestamp in milliseconds
            long startMilliseconds = convertToMilliseconds(start);
            props.put(START_TIMESTAMP, startMilliseconds);
        }

        if (props.get(END_TIMESTAMP) != null && props.get(END_TIMESTAMP) instanceof LocalDateTime) {
            LocalDateTime end = (LocalDateTime) props.get(END_TIMESTAMP);
            long endMilliseconds = convertToMilliseconds(end);
            props.put(END_TIMESTAMP, endMilliseconds);
        }

        Object streamParam = props.remove(STREAM);
        String streamSuffix;
        if (streamParam instanceof List) {
            List<String> streams = ((List<Object>) streamParam).stream().map(String::valueOf).collect(Collectors.toList());
            streamSuffix = String.join("&stream=", streams);
        } else {
            streamSuffix = String.valueOf(streamParam);
        }
        streamSuffix = "&stream=" + streamSuffix;
        String url = this.url + "/search/sse/messages";
        url = urlEncode(url, props) + streamSuffix;

        return new Data(executeSseRequest(url), cache);
    }

    public List<JsonElement> executeSseRequest(String url) {
        Client client = ClientBuilder.newBuilder().build();
        WebTarget target = client.target(url);
        Set<String> sseEventData = new HashSet<>();
        List<JsonElement> result = new ArrayList<>();
        List<String> ignoreEvents = Arrays.asList("close", "keep_alive", "message_ids");
        List<String> errors = new ArrayList<>();
        try (SseEventSource sseEventSource = SseEventSource.target(target).build()) {
            sseEventSource.register(event -> {
                if (event.getName().equals("error")) {
                    throw new HttpError(event.readData());
                }
                if (!ignoreEvents.contains(event.getName())) {
                    sseEventData.add(event.readData());
                }
            }, ex -> {
                errors.add(ex.getMessage());
            });
            sseEventSource.open();
            Thread.sleep(SSE_TIMEOUT);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (!errors.isEmpty()) {
            throw new HttpError(errors.get(0));
        }
        for (String record : sseEventData) {
            result.add(JsonParser.parseString(record).getAsJsonObject());
        }
        return result;
    }

    public Set<JsonObject> findMessagesByIdFromDataProvider(String... messagesId) {
        Set<JsonObject> result = new HashSet<>();
        for (String msgId : messagesId) {
            String url = this.url + "/message/" + msgId;
            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .build();
            AtomicReference<String> responseText = new AtomicReference<>();
            try {
                client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body)
                        .thenAccept(response ->  {
                            responseText.set(response);
                            result.add(JsonParser.parseString(response).getAsJsonObject());
                        })
                        .join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof IllegalStateException ||
                        e.getCause() instanceof JsonSyntaxException) {
                    throw new ValueError("Sorry, but the answer rpt-data-provider doesn't match the json format. " +
                            "Answer: \n " + responseText.get(), e);
                }
                throw e;
            }
        }
        return result;
    }

    public Set<JsonObject> findEventsByIdFromDataProvider(String... eventsId) {
        Set<JsonObject> result = new HashSet<>();
        String eventsSuffix = "?ids=" + String.join("&ids=", eventsId);
        String url = this.url + "/events/" + eventsSuffix;
        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .build();
        AtomicReference<String> responseText = new AtomicReference<>();
        try {
            client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
                    .thenApply(HttpResponse::body)
                    .thenAccept(response ->  {
                        responseText.set(response);
                        JsonArray events = JsonParser.parseString(response).getAsJsonArray();
                        for (JsonElement event : events) {
                            result.add(event.getAsJsonObject());
                        }
                    })
                    .join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof IllegalStateException ||
                    e.getCause() instanceof JsonSyntaxException) {
                throw new ValueError("Sorry, but the answer rpt-data-provider doesn't match the json format. " +
                        "Answer: \n " + responseText.get(), e);
            }
            throw e;
        }
        return result;
    }

    public static Data readCsvFile(String... sources) throws IOException, CsvValidationException {
        JsonArray elements = new JsonArray();
        for (String src : sources) {
            CSVReader reader = new CSVReader(new FileReader(src));
            String[] headers = reader.readNext();
            if (headers != null && headers.length > 0) {
                String[] nextLine;
                while ((nextLine = reader.readNext()) != null) {
                    JsonObject obj = new JsonObject();
                    if (headers.length != nextLine.length) {
                        throw new CsvValidationException("Values and headers length do not match for file " + src);
                    }
                    for (int i = 0; i < headers.length; i++) {
                        obj.addProperty(headers[i], nextLine[i]);
                    }
                    elements.add(obj);
                }
            }
        }
        List<JsonElement> forData = new ArrayList<>();
        for (JsonElement element : elements) {
            forData.add(element);
        }
        return new Data(forData);
    }

}
