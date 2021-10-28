package com.exactpro.th2.dataservices;

public class HttpError extends RuntimeException {

    public HttpError(String message) {
        super(message);
    }

    public HttpError(String message, Throwable cause) {
        super(message, cause);
    }
}
