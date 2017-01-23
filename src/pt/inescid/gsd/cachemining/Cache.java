package pt.inescid.gsd.cachemining;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Cache<T> {

    private final static String CACHE_SIZE_KEY = "cache-size";
    private final static String CACHE_SIZE_DEFAULT = "1000";

    private Logger log = Logger.getLogger(Cache.class);

    private Map<String, Node> cache;

    private int maxSize;

    Node head, tail;

    public Cache(Properties properties) {
        maxSize = Integer.parseInt(properties.getProperty(CACHE_SIZE_KEY, CACHE_SIZE_DEFAULT));
        cache = new HashMap<>(maxSize);

        log.info("Cache (size: " + maxSize + ")");
    }

    public boolean contains(String key) {
        return cache.containsKey(key);
    }

    public CacheEntry<T> get(String key) {
        Node node = cache.get(key);
        if (node == null) {
            return null;
        }

        // move accessed node to the head
        if(!node.key.equals(head.key)) {
            if (node.key.equals(tail.key)) {
                node.previous.next = null;
                tail = node.previous;
            } else  {
                node.previous.next = node.next;
                node.next.previous = node.previous;
            }
            node.previous = null;
            head.previous = node;
            node.next = head;
            head = node;
        }
        return node.entry;
    }

    public void put(String key, CacheEntry<T> entry) {

        Node node = new Node(key, entry);
        if (tail == null) {
            tail = node;
        } else {
            node.next = head;
            head.previous = node;
        }
        head = node;

        cache.put(key, node);
        if(cache.size() > maxSize) {
            cache.remove(tail.key);
            tail = tail.previous;
            tail.next = null;
        }
    }

    @Override
    public String toString() {
        String str = "";

        for (String key : cache.keySet()) {
            str += key + " - ";
        }
        return str;
    }

    private class Node {
        private Node previous = null;
        private Node next;
        private String key;
        private CacheEntry<T> entry;

        public Node(String key, CacheEntry<T> entry) {
            this.key = key;
            this.entry = entry;
        }
    }
}
