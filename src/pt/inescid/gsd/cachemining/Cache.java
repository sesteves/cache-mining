package pt.inescid.gsd.cachemining;

import java.util.HashMap;
import java.util.Map;

public class Cache<T> {

    private Map<String, CacheEntry<T>> cache = new HashMap<>();

    public boolean contains(String key) {
        return cache.containsKey(key);
    }

    public CacheEntry<T> get(String key) {
        return cache.get(key);
    }

    public void put(String key, CacheEntry<T> entry) {
        cache.put(key, entry);
    }

    @Override
    public String toString() {
        String str = "";

        for (String key : cache.keySet()) {
            str += key + " - ";
        }
        return str;
    }
}
