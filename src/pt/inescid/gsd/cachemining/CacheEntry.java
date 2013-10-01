package pt.inescid.gsd.cachemining;

public class CacheEntry<T> {

    private T value;

    public CacheEntry(T value) {
        this.value = value;
    }

    public T getResult() {
        return value;
    }

}
