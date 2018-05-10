package pt.inescid.gsd.cachemining;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class Cache<T> {

    private Logger log = Logger.getLogger(Cache.class);

    private Map<String, Node> cache;

    private int size;

    Node head, tail;

    public Cache(int size) {
        this.size = size;
        cache = new HashMap<>(size);

        log.info("Cache (size: " + size + ")");
    }

    public boolean contains(String key) {
        return cache.containsKey(key);
    }

    public synchronized CacheEntry<T> get(String key) {
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

    public synchronized void put(String key, CacheEntry<T> entry) {

        Node node = new Node(key, entry);
        if (tail == null) {
            tail = node;
        } else {
            node.next = head;
            head.previous = node;
        }
        head = node;

        cache.put(key, node);
        if(cache.size() > size) {
            cache.remove(tail.key);
            tail = tail.previous;
            tail.next = null;
        }
    }

    public synchronized void clear() {
        cache.clear();
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
