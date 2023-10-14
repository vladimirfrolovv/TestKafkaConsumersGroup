//package org.example.handler;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.example.FizzBuzzConsumer;
//
//public class KafkaRecordHandler implements Runnable {
//    private final ConsumerRecord<String, String> record;
////    private final FizzBuzzConsumer fizzBuzzConsumer;
//
//    public KafkaRecordHandler(ConsumerRecord<String, String> record) {
//        this.record = record;
//    }
//
//    @Override
//    public void run ( ) {
//        String value = record.value();
//        int number = Integer.parseInt(value);
//
//        String result = fizzBuzz(number);
////        System.out.println(result);
//        FizzBuzzConsumer..add(number - 1, result);
//    }
//
//    public  String fizzBuzz ( int number ) {
//        if (number % 3 == 0 && number % 5 == 0) {
//            return "fizzbuzz";
//        } else if (number % 3 == 0) {
//            return "fizz";
//        } else if (number % 5 == 0) {
//            return "buzz";
//        } else {
//            return String.valueOf(number);
//        }
//    }
//}
