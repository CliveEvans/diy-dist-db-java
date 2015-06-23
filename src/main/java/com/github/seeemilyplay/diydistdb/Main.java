package com.github.seeemilyplay.diydistdb;

import java.util.List;

/**
 * A Main method you can use as a stub.
 */
public class Main {
    public static void main( String[] args ) throws Exception {
        String[] nodeUrls = new String[]{"http://localhost:8080",
                                         "http://localhost:8081",
                                         "http://localhost:8082"};
        write(nodeUrls, 3, 2, new Thing(3, "foo"));
        write(nodeUrls, 3, 2, new Thing(7, "bar"));
        Thing thing3 = read(nodeUrls, 3);
        Thing thing7 = read(nodeUrls, 7);
        System.out.println(thing3);
        System.out.println(thing7);
    }

    public static void write(String[] nodeUrls,
                             int replicationFactor,
                             int writeConsistency,
                             Thing thing) throws Exception {
        int successCount = 0;
        for (int i=0; i<replicationFactor; i++) {
            try {
                Node.putThing(nodeUrls[i], thing);
                successCount++;
            } catch (Exception e) {
                ; //ignore
            }
        }
        if (successCount < writeConsistency) {
            throw new Exception("Only wrote to " + successCount + " nodes");
        }
    }

    public static Thing read(String[] nodeUrls, int id) throws Exception {
        //todo: only works with one node, need to make distributed!
        return Node.getThing(nodeUrls[0], id);
    }
}
