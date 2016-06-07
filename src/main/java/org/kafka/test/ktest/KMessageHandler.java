package org.kafka.test.ktest;

import java.io.UnsupportedEncodingException;

import com.google.gson.JsonSyntaxException;

public interface KMessageHandler {
    public void handleMessage(byte[] b) throws JsonSyntaxException, UnsupportedEncodingException;
}
