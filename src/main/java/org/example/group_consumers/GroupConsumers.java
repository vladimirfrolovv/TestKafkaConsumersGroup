package org.example.group_consumers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.configs.ConfigConsumer;
import org.example.transform.FizzBuzz;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class GroupConsumers implements Closeable {
    private static final String TOPIC_NAME = "sequence";
    private static final String OUTPUT_FILE = "output.txt";
    private static final long maxSequenceSize = 1000000;
    private static final LongAdder sequenceCounter = new LongAdder();
    private static final List<String> resultList = new ArrayList<>(Collections.nCopies(1000000, "1"));
    private ExecutorService executorService;
    private static final int maxConsumers = 8;

    public void run ( int consumerCount, String consumerGroups ) {
        consumerCount = Math.min(consumerCount, maxConsumers);
        executorService = Executors.newFixedThreadPool(consumerCount);

        for (int i = 0; i < consumerCount; i++) {
            executorService.execute(( ) -> startConsumer(consumerGroups));
        }
    }


    private void startConsumer ( String consumerGroup ) {
        Properties properties = ConfigConsumer.getConsumerProps(consumerGroup);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            while ( sequenceCounter.longValue() != maxSequenceSize ) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

                for (ConsumerRecord<String, String> record : records) {
                    sequenceCounter.increment();
                    String value = record.value();
                    int number = Integer.parseInt(value);
                    String result = FizzBuzz.fizzBuzz(number);
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
            log.error("Error message", e);
        }
    }

    @Override
    public void close ( ) {
        executorService.shutdown();

        try {
            executorService.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.error("Error message", e);
            Thread.currentThread().interrupt();

        }
    }
}
