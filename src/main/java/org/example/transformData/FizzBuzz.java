package org.example.transformData;

public class FizzBuzz {
    public static String fizzBuzz ( int number ) {
//        try {
//            //271565
//            Thread.sleep((Duration.ofNanos(1).toNanos()));
//        } catch (InterruptedException e) {
//            log.error(e.getMessage());
//        }
        if (number % 3 == 0 && number % 5 == 0) {
            return "fizzbuzz";
        } else if (number % 3 == 0) {
            return "fizz";
        } else if (number % 5 == 0) {
            return "buzz";
        } else {
            return String.valueOf(number);
        }
    }
}
