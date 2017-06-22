package pt.inescid.gsd.cachemining;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Sergio on 21/06/2017.
 */
public class PrefetchingContext {

    private int count = 0;

    private int countHits = 0;

    private int currentLevel = 0;

    private Set<DataContainer> prefetched = new HashSet<>();

    private List<Set<DataContainer>> containersPerLevel;

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
}
