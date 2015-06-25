package com.github.seeemilyplay.diydistdb;

import java.util.Collections;
import java.util.Comparator;
import java.util.ArrayList;
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
        Thing thing3 = read(nodeUrls, 3, 2, 3);
        Thing thing7 = read(nodeUrls, 3, 2, 7);
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

    public static Thing read(String[] nodeUrls,
                             int replicationFactor,
                             int readConsistency,
                             int id) throws Exception {
        List<Thing> things = new ArrayList<Thing>();
        for (int i=0; i<replicationFactor; i++) {
            try {
                Thing thing = Node.getThing(nodeUrls[i], id);
                things.add(thing);
                if (things.size() >= readConsistency) {
                  break; //we've got enough successful replies already
                } 
            } catch (Exception e) {
                ; //ignore
            }
        }
        if (things.size() < readConsistency) {
            throw new Exception("Only read from " + things.size() + " nodes");
        }
        return Collections.max(things, new Comparator<Thing>() {
            @Override
            public int compare(Thing first, Thing second) {
                return new Long(first.getTimestamp()).compareTo(
                                                        second.getTimestamp());
            }
        }); 
    }
}
