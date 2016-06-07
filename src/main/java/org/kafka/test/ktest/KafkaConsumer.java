package org.kafka.test.ktest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class KafkaConsumer {
    private static final String TOPIC = "KTest";
    private Properties props = new Properties();
    private ExecutorService kafkaExecutorService;
    private ConsumerConnector consumerConnector;

    public KafkaConsumer(final String zookeeperConnect, int partitions, final KMessageHandler handler) {

        kafkaExecutorService = Executors.newFixedThreadPool(partitions);

        props.put("zookeeper.connect", zookeeperConnect);
        props.put("group.id", "KafkaConsumer");
        props.put("zookeeper.session.timeout.ms", "1000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("client.id", UUID.randomUUID().toString());

        consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

        final Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(TOPIC, new Integer(partitions));

        final Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumerConnector
                .createMessageStreams(topicMap);
        final List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(TOPIC);

        for (final KafkaStream<byte[], byte[]> stream : streamList) {

            kafkaExecutorService.submit(new Runnable() {

                public void run() {

                    final ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
                    while (consumerIte.hasNext()) {

                        final MessageAndMetadata<byte[], byte[]> msgAndMtd = consumerIte.next();
                        try {
                            handler.handleMessage(msgAndMtd.message());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                }

            });
        }
    }

    public void shutdown() throws InterruptedException {
        kafkaExecutorService.shutdownNow();
        kafkaExecutorService.awaitTermination(10L, TimeUnit.SECONDS);

        consumerConnector.shutdown();
    }
}
