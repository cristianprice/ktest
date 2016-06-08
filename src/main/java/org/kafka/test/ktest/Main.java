package org.kafka.test.ktest;

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * Hello world!
 *
 */
public class Main {
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {
		final KafkaConsumer consumer = new KafkaConsumer(args[0], new Integer(args[1]), new KMessageHandler() {

			private Gson gson = new Gson();

			public void handleMessage(byte[] b) throws JsonSyntaxException, UnsupportedEncodingException {
				LOG.info("{}", gson.fromJson(new String(b, "UTF-8"), Object.class));
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
