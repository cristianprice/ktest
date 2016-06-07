package org.kafka.test.ktest;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * Hello world!
 *
 */
public class Main {
    public static void main(String[] args) {
        final KafkaConsumer consumer = new KafkaConsumer(args[0], new Integer(args[1]), new KMessageHandler() {
            private Gson gson = new Gson();

            public void handleMessage(byte[] b) throws JsonSyntaxException, UnsupportedEncodingException {
                System.out.println(gson.fromJson(new String(b, "UTF-8"), Object.class));
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    consumer.shutdown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });
    }
}
