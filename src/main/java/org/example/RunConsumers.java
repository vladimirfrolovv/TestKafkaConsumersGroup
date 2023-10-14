package org.example;

import org.example.groupConsumers.GroupConsumers;

import java.util.UUID;

public class RunConsumers {
    public static void main ( String[] args ) {
        long time = System.currentTimeMillis();
        GroupConsumers.run(8, UUID.randomUUID().toString());
        System.out.println(System.currentTimeMillis() - time);
    }
}
