package pt.inescid.gsd.cachemining.heuristics;

import pt.inescid.gsd.cachemining.DataContainer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by sesteves on 21-06-2017.
 */
public abstract class Heuristic implements Iterator<DataContainer> {
    protected List<Set<DataContainer>> containersPerLevel = new ArrayList<>();

    public List<Set<DataContainer>> getContainersPerLevel() {
        return containersPerLevel;
    }
}
