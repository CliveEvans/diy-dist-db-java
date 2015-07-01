package com.github.seeemilyplay.diydistdb;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

/**
 * A Main method you can use as a stub.
 */
public class Main {


    public static void main( String[] args ) throws Exception {
        List<String> nodeUrls = getNodes();
        NodePool nodePool = new NodePool(nodeUrls);

        nodePool.write(new Thing(3, "foo"));
        Thing baz = new Thing(7, "baz");
        Node.putThing(getNodes().get(1), baz);
        Thread.sleep(500);
        Thing bar = new Thing(7, "bar");
        Node.putThing(getNodes().get(2), bar);
        Node.putThing(getNodes().get(0), bar);

        Thing thing3 = nodePool.getThing(3);
        System.out.println(thing3);
        Thing thing7 = nodePool.getThing(7);
        System.out.println(thing7);

        Thing thing7Fixed = nodePool.getThing(7);
        System.out.println(thing7Fixed);

    }

    private static List<String> getNodes() {
        return asList(8080, 8081, 8082).stream()
                                       .map(port -> String.format("http://localhost:%d", port))
                                       .collect(Collectors.toList());
    }


}
