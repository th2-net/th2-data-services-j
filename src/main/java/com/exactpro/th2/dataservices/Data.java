package com.exactpro.th2.dataservices;

import com.google.gson.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Data implements Iterable<JsonElement> {

    private List<JsonElement> data;

    private boolean cacheStatus;

    private String cacheFilename;

    private Integer length;

    private Integer lengthHint;

    private List<String> parentsCache;

    public Data() {
        this.data = new ArrayList<>();
        this.cacheFilename = "Data_" + UUID.randomUUID() + ":" + System.currentTimeMillis() + ".txt";
        this.cacheStatus = false;
        this.parentsCache = new ArrayList<>();
    }

    public Data(List<JsonElement> data) {
        this.data = data;
        this.length = data.size();
        this.cacheFilename = "Data_" + UUID.randomUUID() + ":" + System.currentTimeMillis() + ".txt";
        this.cacheStatus = false;
        this.parentsCache = new ArrayList<>();
    }

    public Data(List<JsonElement> data, boolean cacheStatus) throws IOException {
        this.data = data;
        this.length = data.size();
        this.cacheFilename = "Data_" + UUID.randomUUID() + ":" + System.currentTimeMillis() + ".txt";
        this.cacheStatus = cacheStatus;
        if (cacheStatus) {
            writeCacheFile();
        }
        this.parentsCache = new ArrayList<>();
    }

    public Data(List<JsonElement> data, List<String> parentsCache) {
        this.data = data;
        this.length = data.size();
        this.cacheFilename = "Data_" + UUID.randomUUID() + ":" + System.currentTimeMillis() + ".txt";
        this.cacheStatus = false;
        this.parentsCache = parentsCache;
    }

    public Data(List<JsonElement> data, boolean cacheStatus, List<String> parentsCache) throws IOException {
        this.data = data;
        this.length = data.size();
        this.cacheFilename = "Data_" + UUID.randomUUID() + ":" + System.currentTimeMillis() + ".txt";
        this.cacheStatus = cacheStatus;
        if (cacheStatus) {
            writeCacheFile();
        }
        this.parentsCache = parentsCache;
    }

    public List<JsonElement> getData() {
        return data;
    }

    public void setData(List<JsonElement> data) {
        this.data = data;
    }

    public Integer getLength() {
        return length;
    }

    public boolean isEmpty() throws IOException {
        return this.loadData(false).isEmpty();
    }

    public Integer getLengthHint() {
        if (this.length != null) {
            return this.length;
        } else return Objects.requireNonNullElse(this.lengthHint, 8192);
    }

    public void setLengthHint(Integer lengthHint) {
        this.lengthHint = lengthHint;
    }

    public List<JsonElement> loadData(boolean cache) throws IOException {
        if (cache && checkCache(cacheFilename)) {
            return loadFile(cacheFilename);
        }
        List<JsonElement> workingData = this.data;
        String cacheFile = getLastCache();
        if (cacheFile != null) {
            workingData = loadFile(cacheFile);
        }
        return workingData;
    }

    public String getLastCache() throws IOException {
        List<String> stack = new ArrayList<>(this.parentsCache);
        //parentsCache works as a stack
        Collections.reverse(stack);
        for (String cacheFilename : stack) {
            if (this.checkCache(cacheFilename)) {
                return cacheFilename;
            }
        }
        return null;
    }

    private boolean checkCache(String filename) throws IOException {
        File current = new File("");
        Path currentPath = Paths.get(current.getAbsolutePath());
        Path path = Paths.get(currentPath.toString(), "temp");
        Files.createDirectories(path);
        Path cache = Paths.get(path.toString(), filename);
        return Files.exists(cache) && Files.isRegularFile(cache);
    }

    private List<JsonElement> loadFile(String filename) throws IOException {
        File current = new File("");
        Path path = Paths.get(current.getAbsolutePath(), "temp", filename);
        if (!Files.exists(path)) {
            throw new ValueError(filename + " doesn't exist.");
        }
        if (!Files.isRegularFile(path)) {
            throw new ValueError(filename + " isn't file.");
        }
        BufferedReader reader = Files.newBufferedReader(path);
        String line = reader.readLine();
        StringBuilder result = new StringBuilder("");
        while (line != null) {
            result.append(line);
            line = reader.readLine();
        }
        List<JsonElement> list = new ArrayList<>();
        JsonArray dataFromFile = JsonParser.parseString(result.toString()).getAsJsonArray();
        for (JsonElement data : dataFromFile) {
            list.add(data.getAsJsonObject());
        }
        return list;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Data data1 = (Data) o;
        if (data.size() != data1.data.size()) {
            return false;
        }
        return data.equals(data1.getData());
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        StringBuilder sb = new StringBuilder("------------- Printed first 5 records -------------\n");
        try {
            this.loadData(false).stream().limit(5).forEach(json -> sb.append(gson.toJson(json)).append("\n"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sb.toString();
    }

    private void writeCacheFile() throws IOException {
        File current = new File("");
        Path writeTo = Paths.get(current.getAbsolutePath(), "temp", this.cacheFilename);
        Files.createDirectories(writeTo.getParent());
        Files.createFile(writeTo);
        PrintWriter file = new PrintWriter(writeTo.toFile().getAbsolutePath());
        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        StringBuilder sb = new StringBuilder("[");
        for (JsonElement element : data) {
            sb.append(gson.toJson(element)).append(",\n");
        }
        if (!data.isEmpty()) {
            sb.deleteCharAt(sb.lastIndexOf(","));
        }
        sb.append("]");
        file.print(sb);
        file.close();
    }

    public Data filter(Predicate<JsonElement> predicate) throws IOException {
        List<String> newParentsCache = new ArrayList<>(parentsCache);
        newParentsCache.add(cacheFilename);
        Data newData = new Data(data
                    .stream()
                    .filter(predicate)
                    .collect(Collectors.toList()),
                newParentsCache
        );
        if (cacheStatus) {
            newData.writeCacheFile();
        }
        return newData;
    }

    public Data map(Function<JsonElement, ? extends JsonElement> mapper) throws IOException {
        List<String> newParentsCache = new ArrayList<>(parentsCache);
        newParentsCache.add(cacheFilename);
        Data newData = new Data(data
                .stream()
                .map(mapper)
                .collect(Collectors.toList()),
                newParentsCache
        );
        if (cacheStatus) {
            newData.writeCacheFile();
        }
        return newData;
    }

    public Data flatMap(Function<JsonElement, Stream<? extends JsonElement>> mapper) throws IOException {
        List<String> newParentsCache = new ArrayList<>(parentsCache);
        newParentsCache.add(cacheFilename);
        Data newData = new Data(data
                .stream()
                .flatMap(mapper)
                .collect(Collectors.toList()),
                newParentsCache
        );
        if (cacheStatus) {
            newData.writeCacheFile();
        }
        return newData;
    }

    public void forEach(Consumer<? super JsonElement> action) {
        try {
            this.loadData(cacheStatus).forEach(action);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Data limit(Integer limit) throws IOException {
        List<String> newParentsCache = new ArrayList<>(parentsCache);
        newParentsCache.add(cacheFilename);
        lengthHint = limit;
        Data newData = new Data(data
                    .stream()
                    .limit(limit)
                    .collect(Collectors.toList()),
                newParentsCache);
        newData.setLengthHint(limit);
        if (cacheStatus) {
            newData.writeCacheFile();
        }
        return newData;
    }

    public Data sift(Integer skip, Integer limit) throws IOException {
        List<String> newParentsCache = new ArrayList<>(parentsCache);
        newParentsCache.add(cacheFilename);
        Stream<JsonElement> dataStream = data.stream();
        if (skip != null) {
            dataStream = dataStream.skip(skip);
        }
        if (limit != null) {
            dataStream = dataStream.limit(limit);
        }
        Data newData = new Data(dataStream.collect(Collectors.toList()), newParentsCache);
        if (cacheStatus) {
            newData.writeCacheFile();
        }
        return newData;
    }

    public Iterator<JsonElement> iterator() {
        try {
            return this.loadData(cacheStatus).iterator();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Data useCache(boolean status) {
        this.cacheStatus = status;
        return this;
    }

    public List<JsonElement> findBy(String recordField, List<?> fieldValues) {
        List<JsonElement> found = new ArrayList<>();
        fieldValues = fieldValues.stream().map(Object::toString).collect(Collectors.toList());
        for (JsonElement record : this) {
            if (fieldValues.contains(record.getAsJsonObject().getAsJsonPrimitive(recordField).getAsString())) {
                found.add(record);
            }
        }
        return found;
    }

    public void writeToFile(String source) throws IOException {
        PrintWriter file = new PrintWriter(source);
        String delimiter = "-".repeat(50);
        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        for (JsonElement element : data) {
            file.println(gson.toJson(element) + "\n" + delimiter);
        }
        file.close();
    }
}
