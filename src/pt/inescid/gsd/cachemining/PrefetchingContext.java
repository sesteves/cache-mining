package pt.inescid.gsd.cachemining;

import pt.inescid.gsd.cachemining.heuristics.FetchProgressively;
import pt.inescid.gsd.cachemining.heuristics.Heuristic;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Created by Sergio on 21/06/2017.
 */
public class PrefetchingContext {

    private String id = UUID.randomUUID().toString();

    private int count = 0;

    private int countHits = 0;

    private int currentLevel = 0;

    private Set<DataContainer> prefetched = new HashSet<>();

    private List<Set<DataContainer>> containersPerLevel;

    private Heuristic iterator;

    private DataContainer lastRequestedDc;

    public PrefetchingContext(Heuristic iterator) {
        if(iterator instanceof FetchProgressively) {
            this.iterator = iterator;
        }
        this.containersPerLevel = iterator.getContainersPerLevel();
    }

    public void add(DataContainer dc) {
        prefetched.add(dc);
        count++;
    }

    public boolean remove(DataContainer dc) {
        if(prefetched.remove(dc)) {
            countHits++;
            return true;
        }
        return false;
    }

    public boolean matches(DataContainer dc) {
        if(currentLevel == containersPerLevel.size()) {
            return false;
        }
        boolean result = containersPerLevel.get(currentLevel).contains(dc);
        currentLevel++;
        return result;
    }

    public void setContainersPerLevel(List<Set<DataContainer>> containersPerLevel) {
        this.containersPerLevel = containersPerLevel;
    }

    public int getCount() {
        return count;
    }

    public Heuristic getIterator() {
        return iterator;
    }

    public void setLastRequestedDc(DataContainer lastRequestedDc) {
        this.lastRequestedDc = lastRequestedDc;
    }

    public DataContainer getLastRequestedDc() {
        return lastRequestedDc;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PrefetchingContext that = (PrefetchingContext) o;

        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
