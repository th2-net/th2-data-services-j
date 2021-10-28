package com.exactpro.th2.dataservices;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataServicesUtils {

    // th2-kube-demo  Host port where rpt-data-provider is located.
    public static final String DEMO_HOST = "10.64.66.66";

    //Node port of rpt-data-provider.
    public static final String DEMO_PORT = "30999";

    //request parameters
    public static final String START_TIMESTAMP = "startTimestamp",
            RESUME_FROM_ID = "resumeFromId",
            END_TIMESTAMP = "endTimestamp",
            STREAM = "stream",
            METADATA_ONLY = "metadataOnly",
            ATTACHED_MESSAGES = "attachedMessages",
            ROUTE = "route";

    //events and messages fields
    public static final String ATTACHED_MESSAGE_ID = "attachedMessageIds",
            BODY = "body",
            EVENT_NAME = "eventName",
            SUCCESSFUL = "successful",
            BATCH_ID = "batchId",
            EVENT_TYPE = "eventType",
            EVENT_ID = "eventId";

    //millis
    public static final int SSE_TIMEOUT = 5000;


    public static long convertToMilliseconds(LocalDateTime date) {
        return (date.toEpochSecond(
                ZoneId.systemDefault().getRules().getOffset(date)
        ) * 1000000000 + date.getNano()) / 1000000;
    }

    public static String urlEncode(String url, Map<String, Object> params) {
        List<NameValuePair> nameValuePairs = new ArrayList<>(params.size());
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            nameValuePairs.add(new BasicNameValuePair(entry.getKey(), String.valueOf(entry.getValue())));
        }
        return url + "?" + URLEncodedUtils.format(nameValuePairs, "UTF-8");
    }

}
