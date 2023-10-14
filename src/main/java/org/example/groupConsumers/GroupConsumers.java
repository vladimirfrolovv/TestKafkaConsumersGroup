package org.example.groupConsumers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.configs.ConfigConsumer;
import org.example.transformData.FizzBuzz;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class GroupConsumers {
    private final static String TOPIC_NAME = "sequence";
    private static final String OUTPUT_FILE = "output.txt";
    private static final int totalMsgToSend = 1000000;
    private static final AtomicInteger sequence_counter = new AtomicInteger(0);
    public static List<String> resultList = new ArrayList<>(Collections.nCopies(1000000, "1"));

    public static void run ( int consumerCount, String consumerGroups ) {

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_FILE))) {
            ExecutorService executorService = Executors.newFixedThreadPool(consumerCount);

            for (int i = 0; i < consumerCount; i++) {
                executorService.execute(( ) -> startConsumer(consumerGroups));
            }

            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.MINUTES);

            for (String s : resultList) {
//            System.out.println(s);
                writer.write(s);
                writer.newLine();
            }

        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage());
        }
    }


    private static void startConsumer ( String consumerGroup ) {

        Properties properties = ConfigConsumer.getConsumerProps(consumerGroup);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            while ( true ) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

                for (ConsumerRecord<String, String> record : records) {
                    sequence_counter.incrementAndGet();
                    String value = record.value();
                    int number = Integer.parseInt(value);
                    String result = FizzBuzz.fizzBuzz(number);
                    resultList.set(number - 1, result);
                }

                consumer.commitSync();

                if (sequence_counter.get() == totalMsgToSend) {
                    break;
                }
            }
        }
    }


}
