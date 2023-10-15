package org.example;

import org.example.groups.consumers.GroupConsumers;
import org.example.transform.FizzBuzz;
import org.example.transform.Transformation;

import java.io.IOException;
import java.util.UUID;

public class RunConsumers {
    public static void main ( String[] args ) throws IOException {
        long time = System.currentTimeMillis();
        Transformation transformation = new FizzBuzz();
        GroupConsumers groupConsumers = new GroupConsumers(8, UUID.randomUUID().toString(), transformation);
        groupConsumers.run();
        groupConsumers.writeDataToFile();
        groupConsumers.close();
        System.out.println(System.currentTimeMillis() - time);
    }
}
