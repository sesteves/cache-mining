package pt.inescid.gsd.cachemining;

public class CacheEntry<T> {

    private T value;

    private boolean fromPrefetch = false;

    private boolean prefetchHit = false;

    public CacheEntry(T value) {
        this.value = value;
    }

    public CacheEntry(T value, boolean fromPrefetch) {
        this.value = value;
        this.fromPrefetch = fromPrefetch;
    }

    public T getValue() {
        return value;
    }

    public boolean isFromPrefetch() {
        return fromPrefetch;
    }

    public boolean isPrefetchHit() {
        return prefetchHit;
    }

    public void setPrefetchHit() {
        prefetchHit = true;
    }
}
