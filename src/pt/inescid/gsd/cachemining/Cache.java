package pt.inescid.gsd.cachemining;

import java.util.HashMap;
import java.util.Map;

public class Cache<T> {

    private Map<String, CacheEntry<T>> cache = new HashMap<String, CacheEntry<T>>();

    public CacheEntry<T> get(String key) {
        return cache.get(key);
    }

    public void put(String key, CacheEntry<T> entry) {
        cache.put(key, entry);
    }
}
