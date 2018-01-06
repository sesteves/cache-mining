package pt.inescid.gsd.cachemining;

public class CacheEntry<T> {

    private T value;

    public CacheEntry() {
    }

    public CacheEntry(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}
