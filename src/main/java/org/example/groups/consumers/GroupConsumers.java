package org.example.groups.consumers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.configs.ConfigConsumer;
import org.example.transform.Transformation;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class GroupConsumers implements Closeable {
    private static final String TOPIC_NAME = "sequence";
    private static final String OUTPUT_FILE = "output.txt";
    private static final long MAX_SEQUENCE_SIZE = 1000000;
    private static final int MAX_CONSUMERS = 8;
    private final LongAdder sequenceCounter = new LongAdder();
    private final List<String> resultList = new ArrayList<>(Collections.nCopies(1000000, "1"));
    private final ExecutorService executorService;
    private final Transformation transformation;
    private final int consumersCount;
    private final String consumerGroups;


    public GroupConsumers ( int consumersCount, String consumerGroups, Transformation transformation ) {
        this.transformation = transformation;
        this.consumersCount = Math.min(consumersCount, MAX_CONSUMERS);
        this.executorService = Executors.newFixedThreadPool(consumersCount);
        this.consumerGroups = consumerGroups;
    }

    public void run ( ) {
        List<Future<?>> futureList = new ArrayList<>();

        for (int i = 0; i < consumersCount; i++) {
            futureList.add(executorService.submit(( ) -> startConsumer(consumerGroups)));
        }

        for (Future<?> future : futureList) {
            try {
                future.get();
            } catch (ExecutionException e) {
                log.error("Error in data processing", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Error in data processing", e);
            }
        }
    }


    private void startConsumer ( String consumerGroup ) {
        Properties properties = ConfigConsumer.getConsumerProps(consumerGroup);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            while ( sequenceCounter.longValue() < MAX_SEQUENCE_SIZE ) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

                for (ConsumerRecord<String, String> record : records) {
                    sequenceCounter.increment();
                    String value = record.value();
                    int number = Integer.parseInt(value);
                    String result = transformation.transform(number);
                    resultList.set(number - 1, result);
                }
                consumer.commitSync();
            }
        }
    }

    public void writeDataToFile ( ) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_FILE))) {

            for (String s : resultList) {
//            System.out.println(s);
                writer.write(s);
                writer.newLine();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Dont write data", e);
        }
    }

    @Override
    public void close ( ) {
        if (executorService != null) {
            executorService.shutdown();
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Thread pool interrupted", e);
                Thread.currentThread().interrupt();
            }
        }
    }
}
