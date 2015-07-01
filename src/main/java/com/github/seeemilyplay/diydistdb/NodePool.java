package com.github.seeemilyplay.diydistdb;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class NodePool {

    public static final int REPLICATION_FACTOR = 3;
    public static final int WRITE_CONSISTENCY_LEVEL = 2;
    public static final int READ_CONSISTENCY_LEVEL = WRITE_CONSISTENCY_LEVEL;
    private List<String> nodeUrls;

    public NodePool(List<String> nodeUrls) {
        this.nodeUrls = nodeUrls;
    }

    public void write(Thing thing) throws Exception {

        List<Exception> failures = overNodes()
                                             .flatMap(u -> writeToSingleNode(thing, u))
                                             .collect(toList());
        int successes = countSuccesses(failures);
        if(successes >= WRITE_CONSISTENCY_LEVEL) {
            System.out.println("Successfully wrote " + successes + " nodes");
        } else {
            System.err.println("Failed to write " + thing + " to enough nodes!");
            System.err.println("Errors: " + failures);
            throw new Exception();
        }
    }

    public Thing getThing(int id) throws Exception {
        List<NodeThing> fetched = overNodes()
                .flatMap(nodeUrls -> getThingFromSingle(id, nodeUrls))
                .collect(toList());

        Map<Thing, Long> fetchedByCount = fetched.stream()
                .collect(Collectors.groupingBy(o -> o.thing, Collectors.counting()));

        System.out.println(fetchedByCount);
        Map.Entry<Thing, Long> mostRecent = fetchedByCount.entrySet().stream()
                                                       .sorted(compareTimestamps())
                                                       .findFirst()
                                                       .get();
        Long mostRecentCount = mostRecent.getValue();
        Thing mostRecentThing = mostRecent.getKey();
        if(mostRecentCount < READ_CONSISTENCY_LEVEL) {
            throw new Exception("Failed to read sufficient copies. Only got: " + mostRecentCount + " copy of the most recent version " + mostRecentThing);
        }
        if(fetchedByCount.size() > 1) {
            attemptToRepair(mostRecentThing, fetched);
        }
        return mostRecentThing;
    }

    private void attemptToRepair(Thing correctValue, List<NodeThing> fetchedByCount) {
        System.out.println("Attempting to repair " + fetchedByCount);
        fetchedByCount.stream()
                .filter(e -> !e.thing.equals(correctValue))
                .map(NodeThing::getNodeUrl)
                .forEach(url -> writeToSingleNode(correctValue, url))
        ;
    }

    private Comparator<Map.Entry<Thing, Long>> compareTimestamps() {
        return (e1, e2) -> -(int)(e1.getKey().getTimestamp()- e2.getKey().getTimestamp());
    }

    private Stream<NodeThing> getThingFromSingle(int id, String nodeUrl) {
        try {
            return Stream.of(new NodeThing(nodeUrl, Node.getThing(nodeUrl, id)));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return Stream.empty();
        }
    }

    private int countSuccesses(List<Exception> failures) {
        return REPLICATION_FACTOR - failures.size();
    }

    private Stream<String> overNodes() {
        return nodeUrls.stream().limit(REPLICATION_FACTOR);
    }


    private Stream<Exception> writeToSingleNode(Thing thing, String url) {
        try {
            Node.putThing(url, thing);
            return Stream.empty();
        } catch (Exception e) {
            System.out.println("Failed to write " + thing + " to " + url);
            System.out.println(e.getMessage());
            return Stream.of(e);
        }
    }

    private static class NodeThing {
        private final String nodeUrl;
        private final Thing thing;

        private NodeThing(String nodeUrl, Thing thing) {
            this.nodeUrl = nodeUrl;
            this.thing = thing;
        }

        public String getNodeUrl() {
            return nodeUrl;
        }

        @Override
        public String toString() {
            return "NodeThing{" +
                    "nodeUrl='" + nodeUrl + '\'' +
                    ", thing=" + thing +
                    '}';
        }
    }
}
