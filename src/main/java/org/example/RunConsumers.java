package org.example;

import org.example.group_consumers.GroupConsumers;

import java.util.UUID;

public class RunConsumers {
    public static void main ( String[] args ) {
        long time = System.currentTimeMillis();
        GroupConsumers groupConsumers = new GroupConsumers();
        groupConsumers.run(8, UUID.randomUUID().toString());
        groupConsumers.close();
        groupConsumers.writeDataToFile();
        System.out.println(System.currentTimeMillis() - time);
    }
}
