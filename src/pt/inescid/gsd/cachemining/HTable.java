package pt.inescid.gsd.cachemining;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import pt.inescid.gsd.cachemining.heuristics.FetchProgressively;
import pt.inescid.gsd.cachemining.heuristics.Heuristic;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class HTable implements HTableInterface {

    private final static int NUMBER_OF_THREADS = 8;

    private final static String PROPERTIES_FILE = "cachemining.properties";

    private final static String MONITORING_KEY = "monitoring";
    private final static String ENABLED_KEY = "enabled";
    private final static String CACHE_SIZE_KEY = "cache-size";
    private final static String HEURISTIC_KEY = "heuristic";
    private final static String SEQUENCES_FILE_KEY = "sequences-file";

    private static long ts = System.currentTimeMillis();

    private static final String statsFName = String.format("stats-cache-%d.csv", ts);

    private static final String STATS_HEADER = "cachesize,ngets,hits,negets,npfetch,hitpfetch";

    private Logger log = Logger.getLogger(HTable.class);

    private static Map<String, org.apache.hadoop.hbase.client.HTable> htables = new HashMap<String, org.apache.hadoop.hbase.client.HTable>();

    private static Cache<Result> cache;

    private static SequenceEngine sequenceEngine;

    private org.apache.hadoop.hbase.client.HTable htable;

    private static BufferedWriter getOpsF, putOpsF;

    private String tableName;

    private static BufferedWriter statsF;

    private static int countGets = 0, countCacheHits = 0, countFetch = 0, countPrefetch = 0,
            countPrefetchHits = 0;

    private boolean isMonitoring;
    private boolean isEnabled;

    private String statsPrefix;

    private static Queue<DataContainer> prefetchQueue = new ConcurrentLinkedQueue<>();

    private static Queue<PrefetchingContext> prefetchWithContextQueue = new ConcurrentLinkedQueue<>();

    private static final Semaphore prefetchSemaphore = new Semaphore(0);

    private static final Semaphore prefetchWithContextSemaphore = new Semaphore(0);

    private Thread prefetch = new Thread(new Runnable() {
        @Override
        public void run() {
            prefetch();
        }
    });

    private Thread prefetchWithContext = new Thread(new Runnable() {
        @Override
        public void run() {
            prefetchWithContext();
        }
    });

    private static ExecutorService executorPrefetch, executorPrefetchWithContext;

    private static List<PrefetchingContext> activeContexts = new ArrayList<>();

    private static Object activeContextsLock = new Object();

    private static Set<String> prefetchSet = ConcurrentHashMap.newKeySet();

    public HTable(Configuration conf, String tableName) throws IOException {
        Properties properties = init(conf, tableName);
        if(isEnabled && !isMonitoring) {
            String heuristic = System.getProperty(HEURISTIC_KEY, properties.getProperty(HEURISTIC_KEY));
            String sequencesFName = System.getProperty(SEQUENCES_FILE_KEY, properties.getProperty(SEQUENCES_FILE_KEY));
            sequenceEngine = new SequenceEngine(heuristic, sequencesFName);
        }
    }

    public HTable(Configuration conf, String tableName, List<List<DataContainer>> sequences) throws IOException {
        Properties properties = init(conf, tableName);
        if (isEnabled && !isMonitoring) {
            String heuristic = System.getProperty(HEURISTIC_KEY, properties.getProperty(HEURISTIC_KEY));
            sequenceEngine = new SequenceEngine(heuristic, sequences);
        }
    }

    public Properties init(Configuration conf, String tableName) throws IOException {
        PropertyConfigurator.configure("cachemining-log4j.properties");

        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(PROPERTIES_FILE));
        } catch (IOException e) {
            log.info("Could not load properties file '" + PROPERTIES_FILE + "'.");
        }

        // HTable properties
        isMonitoring = Boolean.parseBoolean(System.getProperty(MONITORING_KEY, properties.getProperty(MONITORING_KEY)));
        isEnabled = Boolean.parseBoolean(System.getProperty(ENABLED_KEY, properties.getProperty(ENABLED_KEY)));

        log.info("HTable (Enabled: " + isEnabled + ", isMonitoring: " + isMonitoring + ")");

        // cache properties
        int cacheSize = Integer.parseInt(System.getProperty(CACHE_SIZE_KEY, properties.getProperty(CACHE_SIZE_KEY)));

        this.tableName = tableName;
        htable = new org.apache.hadoop.hbase.client.HTable(conf, tableName);
        htables.put(tableName, htable);

        if(isEnabled) {
            if(isMonitoring) {
                FileWriter putFW = new FileWriter(String.format("put-ops-%d.txt", ts));
                putOpsF = new BufferedWriter(putFW);
                FileWriter getFW = new FileWriter(String.format("get-ops-%d.txt", ts));
                getOpsF = new BufferedWriter(getFW);
            } else {

                cache = new Cache<>(cacheSize);

                executorPrefetch = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
                executorPrefetchWithContext = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
                for (int i = 0; i < NUMBER_OF_THREADS; i++) {
                    executorPrefetch.execute(prefetch);
                    executorPrefetchWithContext.execute(prefetchWithContext);
                }


                statsF = new BufferedWriter(new FileWriter(statsFName));
                statsF.write(STATS_HEADER);
                statsF.newLine();
                statsPrefix = cacheSize + ",";
            }
        }

        return properties;
    }

    public void markTransaction() throws IOException {
        if (!(isEnabled && isMonitoring))
            return;

        getOpsF.write("### TRANSACTION " + System.currentTimeMillis());
        getOpsF.newLine();
    }

    public void setScannerCaching(int scannerCaching) {
        htable.setScannerCaching(scannerCaching);
    }

    @Override
    public Result append(Append arg0) throws IOException {
        return htable.append(arg0);
    }

    @Override
    public Object[] batch(List<? extends Row> arg0) throws IOException, InterruptedException {
        return htable.batch(arg0);
    }

    @Override
    public <R> void batchCallback(List<? extends Row> list, Object[] objects, Callback<R> callback) throws IOException, InterruptedException {

    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> list, Callback<R> callback) throws IOException, InterruptedException {
        return new Object[0];
    }

    @Override
    public void batch(List<? extends Row> arg0, Object[] arg1) throws IOException, InterruptedException {
        htable.batch(arg0, arg1);
    }

    @Override
    public boolean checkAndDelete(byte[] arg0, byte[] arg1, byte[] arg2, byte[] arg3, Delete arg4) throws IOException {
        return htable.checkAndDelete(arg0, arg1, arg2, arg3, arg4);
    }

    @Override
    public boolean checkAndDelete(byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, byte[] bytes3, Delete delete) throws IOException {
        return false;
    }

    @Override
    public boolean checkAndPut(byte[] arg0, byte[] arg1, byte[] arg2, byte[] arg3, Put arg4) throws IOException {
        return htable.checkAndPut(arg0, arg1, arg2, arg3, arg4);
    }

    @Override
    public boolean checkAndPut(byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, byte[] bytes3, Put put) throws IOException {
        return false;
    }

    @Override
    public void close() throws IOException {
        htable.close();
        if (isEnabled) {
            if(isMonitoring) {
                putOpsF.close();
                getOpsF.close();
            } else {
                statsF.close();
                executorPrefetch.shutdownNow();
                executorPrefetchWithContext.shutdownNow();
            }
        }
        log.debug("Table closed: " + tableName);
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] bytes) {
        return htable.coprocessorService(bytes);
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> aClass, byte[] bytes, byte[] bytes1, Call<T, R> call) throws ServiceException, Throwable {
        return htable.coprocessorService(aClass, bytes, bytes1, call);
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> aClass, byte[] bytes, byte[] bytes1, Call<T, R> call, Callback<R> callback) throws ServiceException, Throwable {
        htable.coprocessorService(aClass, bytes, bytes1, call, callback);
    }

    @Override
    public void delete(Delete arg0) throws IOException {
        htable.delete(arg0);
    }

    @Override
    public void delete(List<Delete> arg0) throws IOException {
        htable.delete(arg0);
    }

    @Override
    public boolean exists(Get arg0) throws IOException {
        return htable.exists(arg0);
    }

    @Override
    public boolean[] existsAll(List<Get> list) throws IOException {
        return new boolean[0];
    }

    @Override
    public void flushCommits() throws IOException {
        htable.flushCommits();
    }

    // for debugging purposes
    private String getColumnsStr(Map<byte[], NavigableSet<byte[]>> familyMap) {
        StringBuilder sb = new StringBuilder();
        Set<byte[]> families = familyMap.keySet();
        for (byte[] family : families) {
            sb.append(Bytes.toString(family));
            NavigableSet<byte[]> qualifiers = familyMap.get(family);
            if (qualifiers != null)
                for (byte[] qualifier : qualifiers)
                    sb.append(":" + Bytes.toString(qualifier));
        }
        return sb.toString();
    }

    private void prefetchWithContext() {
        try {
            while (true) {
                prefetchWithContextSemaphore.acquire();
                PrefetchingContext context = prefetchWithContextQueue.poll();

                FetchProgressively iterator = (FetchProgressively) context.getIterator();
                if (!iterator.unblock(context.getLastRequestedDc())) {
                    continue;
                }

                List<Get> gets = new ArrayList<>();
                while (iterator.hasNext()) {
                    DataContainer dc = iterator.next();

                    // if data container is already cached, skip it
                    if (cache.contains(dc.toString())) {
                        continue;
                    }

                    Get get = new Get(dc.getRow());
                    if (dc.getQualifier() != null) {
                        get.addColumn(dc.getFamily(), dc.getQualifier());
                    } else {
                        get.addFamily(dc.getFamily());
                    }
                    gets.add(get);

                    context.add(dc);
                    countPrefetch++;
                }

                // prefetching
                // FIXME only considers one table
                Result[] results = htable.get(gets);
                for (Result result : results) {
                    CacheEntry cacheEntry = new CacheEntry(result);
                    Set<Map.Entry<byte[], NavigableMap<byte[], byte[]>>> mapEntries = result.getNoVersionMap().entrySet();
                    for(Map.Entry<byte[], NavigableMap<byte[], byte[]>> mapEntry : mapEntries) {
                        byte[] family = mapEntry.getKey();
                        // FIXME
//                        if(!hasQualifier) {
//                            String key = DataContainer.getKey(tableName, result.getRow(), family);
//                            log.debug("Adding key to cache: " + key);
//                            cache.put(key, cacheEntry);
//                        }
                        Set<byte[]> qualifiers = mapEntry.getValue().keySet();
                        for(byte[] qualifier : qualifiers) {
                            String key = DataContainer.getKey(tableName, result.getRow(), family, qualifier);
                            log.debug("Adding key to cache: " + key);
                            cache.put(key, cacheEntry);
                        }

                    }
                }
            }
        } catch (InterruptedException e) {
            log.error("Exception in prefetchWithContext: " + e.getMessage());
        } catch (IOException e) {
            log.error("Exception in prefetchWithContext: " + e.getMessage());
        }

    }

    private Result fetchFromCache(DataContainer dc) {
        Result result = null;

        // CONTEXT
        boolean prefetchHit = false;
        List<PrefetchingContext> toRemove = new ArrayList<>();


        if(prefetchSet.remove(dc.toString())) {
            countPrefetchHits++;
            prefetchHit = true;
        }

//        synchronized (activeContextsLock) {
//            log.debug("Number of active contexts: " + activeContexts.size());
//            for (PrefetchingContext context : activeContexts) {
//                if (context.matches(dc)) {
//                    log.debug("There is a context match for dc: " + dc.toString());
//                    if (context.remove(dc)) {
//                        countPrefetchHits++;
//                        prefetchHit = true;
//                    }
//
//                    // if there is an iterator, it means that we are using progressive fetching
//                    if (context.getIterator() != null) {
//                        context.setLastRequestedDc(dc);
//                        prefetchWithContextQueue.add(context);
//                        prefetchWithContextSemaphore.release();
//                    }
//                } else {
//                    toRemove.add(context);
//                }
//            }
//            activeContexts.removeAll(toRemove);
//        }
        String key = dc.toString();
        log.debug("Getting key from cache: " + key);
        // if there is a prefetch hit, then actively wait until element is in cache
        CacheEntry<Result> entry;
        do {
            entry = cache.get(key);
        } while (prefetchHit && entry == null);
        if (entry != null) {
            countCacheHits++;
            result = new Result();
            result.copyFrom(entry.getValue());
        }

        return result;
    }


    private void prefetch() {

        try {
            while (true) {

                prefetchSemaphore.acquire();
                DataContainer dc = prefetchQueue.poll();

                long startTick = System.currentTimeMillis();

                // get sequences matching firstItem
                Heuristic itemsIt = sequenceEngine.getSequences(dc);
                if (itemsIt == null) {
                    log.debug("There is no sequence indexed by key '" + dc + "'.");
                    continue;
                }
                log.debug("There are sequences indexed by key '" + dc + "'.");

                // TODO uncomment
//                // creates prefetching context
//                PrefetchingContext context = new PrefetchingContext(itemsIt);
//                synchronized (activeContextsLock) {
//                    activeContexts.add(context);
//                }
                // context.setContainersPerLevel(itemsIt.getContainersPerLevel());

//                while(itemsIt.hasNext()) {
//                    DataContainer item = itemsIt.next();
//
//                    if (cache.contains(item.toString())) {
//                        continue;
//                    }
//
//                    Get get = new Get(item.getRow());
//                    if(item.hasQualifier()) {
//                        get.addColumn(item.getFamily(), item.getQualifier());
//                    } else {
//                        get.addFamily(item.getFamily());
//                    }
//                    Result result = htables.get(item.getTableStr()).get(get);
//                    CacheEntry entry = new CacheEntry(result);
//                    log.debug("Adding key to cache: " + item.toString());
//                    cache.put(item.toString(), entry);
//
//                    if(!item.hasQualifier()) {
//                        Set<byte[]> qualifiers = result.getFamilyMap(item.getFamily()).keySet();
//                        for(byte[] qualifier : qualifiers) {
//                            String key = DataContainer.getKey(item.getTableStr(), item.getRow(), item.getFamily(), qualifier);
//                            log.debug("Adding key to cache: " + key);
//                            cache.put(key, entry);
//                        }
//                    }
//
//                    countPrefetch++;
//                    context.add(item);
//                }

                // batch updates to the same tables
                Map<String, List<Get>> gets = new HashMap<>();
                boolean hasQualifier = false;
                while (itemsIt.hasNext()) {
                    DataContainer item = itemsIt.next();
                    hasQualifier = item.getQualifier() != null;

                    log.debug("Key to prefetch: " + item.toString());

                    // make sure current item is not part of the current get request

                    if (cache.contains(item.toString())) {
                        continue;
                    }
                    prefetchSet.add(item.toString());

                    List<Get> tableGets = gets.get(item.getTableStr());
                    if (tableGets == null) {
                        tableGets = new ArrayList<>();
                        gets.put(item.getTableStr(), tableGets);
                    }
                    Get g = new Get(item.getRow());
                    if (item.hasQualifier()) {
                        g.addColumn(item.getFamily(), item.getQualifier());
                    } else {
                        g.addFamily(item.getFamily());
                    }
                    tableGets.add(g);

                    countPrefetch++;
                    // TODO uncomment
//                    context.add(item);
                }

//                if(context.getCount() == 0) {
//                    activeContexts.remove(context);
//                }

                // TODO prefetch elements in order
                // prefetch elements to cache
                for (Map.Entry<String, List<Get>> entry : gets.entrySet()) {
                    String tableName = entry.getKey();
                    Result[] results = htables.get(tableName).get(entry.getValue());

                    for (Result result : results) {
                        CacheEntry cacheEntry = new CacheEntry(result);
                        Set<Map.Entry<byte[], NavigableMap<byte[], byte[]>>> mapEntries = result.getNoVersionMap().entrySet();
                        for(Map.Entry<byte[], NavigableMap<byte[], byte[]>> mapEntry : mapEntries) {
                            byte[] family = mapEntry.getKey();
                            if(!hasQualifier) {
                                String key = DataContainer.getKey(tableName, result.getRow(), family);
                                log.debug("Adding key to cache: " + key);
                                cache.put(key, cacheEntry);
                                prefetchSet.remove(key);
                            }
                            Set<byte[]> qualifiers = mapEntry.getValue().keySet();
                            for(byte[] qualifier : qualifiers) {
                                String key = DataContainer.getKey(tableName, result.getRow(), family, qualifier);
                                log.debug("Adding key to cache: " + key);
                                cache.put(key, cacheEntry);
                                prefetchSet.remove(key);
                            }

                        }

                    }
                }

                long diff = System.currentTimeMillis() - startTick;
                log.debug("Time taken with prefetching: " + diff);

            }
        } catch (InterruptedException e) {
            log.error("Exception occurred in prefetch(): " + e.getMessage());
        } catch (IOException e) {
            log.error("Exception occurred in prefetch(): " + e.getMessage());
        }
    }

    private String bytesToStr(byte[] arr) {
        StringBuilder sb = new StringBuilder();
        for (byte b : arr)
            sb.append(String.format("\\x%02x", b & 0xFF));
        return sb.toString();
    }

    private byte[] strToBytes(String str) {
        byte[] result = new byte[str.length() / 4];
        for (int i = 2, j = 0; i < str.length(); i += 4, j++)
            result[j] = (byte) Integer.parseInt(str.substring(i, i + 2), 16);
        return result;
    }


    private void monitorGet(Get get) throws IOException {
        long ts = System.currentTimeMillis();
        String rowStr = "" + Bytes.toHex(get.getRow());
        Set<byte[]> families = get.familySet();
        for (byte[] f : families) {
            NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(f);
            if (qualifiers != null) {
                for (byte[] q : qualifiers) {
                    getOpsF.write(ts + ":" + tableName + ":" + rowStr + ":" + Bytes.toString(f) + ":" + Bytes.toString(q));
                    getOpsF.newLine();
                }
            } else {
                getOpsF.write(ts + ":" + tableName + ":" + rowStr + ":" + Bytes.toString(f));
                getOpsF.newLine();
            }
        }
    }

    @Override
    public Result get(Get get) throws IOException {
        log.debug("get CALLED (" + tableName + ":" + Bytes.toHex(get.getRow()) + ":"
                + getColumnsStr(get.getFamilyMap()) + ")");

        // FIXME do not unfold gets
        // unfolding get in case there is more than one family or qualifier
        Set<Map.Entry<byte[], NavigableSet<byte[]>>> entries = get.getFamilyMap().entrySet();
        Map.Entry<byte[], NavigableSet<byte[]>> first = entries.iterator().next();

        if(get.numFamilies() > 1 || (first.getValue() != null && first.getValue().size() > 1)) {
            List<Cell> cells = new ArrayList<>();
            for (Map.Entry<byte[], NavigableSet<byte[]>> entry : entries) {

                if (entry.getValue() == null) {
                    Get g = new Get(get.getRow());
                    g.addFamily(entry.getKey());

                    Result r = get(g);
                    cells.addAll(r.listCells());
                } else {
                    for (byte[] qualifier : entry.getValue()) {
                        Get g = new Get(get.getRow());
                        g.addColumn(entry.getKey(), qualifier);

                        Result r = get(g);
                        cells.addAll(r.listCells());
                    }
                }
            }

            return Result.create(cells);
        }


        if (!isEnabled) {
            return htable.get(get);
        }
        if (isMonitoring) {
            monitorGet(get);
            return htable.get(get);
        }

        DataContainer dc;
        if(first.getValue() != null) {
            dc = new DataContainer(getTableName(), get.getRow(), first.getKey(), first.getValue().iterator().next());
        } else {
            dc = new DataContainer(getTableName(), get.getRow(), first.getKey());
        }

        // fetch items from cache
        Result result = fetchFromCache(dc);

        if(result == null) {
            // prefetch sequences in the background asynchronously
            prefetchQueue.add(dc);
            prefetchSemaphore.release();

            countFetch++;
            result = htable.get(get);

            // add fetched result to cache
            Result resultClone = new Result();
            resultClone.copyFrom(result);
            CacheEntry entry = new CacheEntry<>(resultClone);
            cache.put(dc.toString(), entry);

            if(!dc.hasQualifier()) {
                for(byte[] qualifier : result.getFamilyMap(dc.getFamily()).keySet()) {
                    String key = DataContainer.getKey(dc.getTableStr(), dc.getRow(), dc.getFamily(), qualifier);
                    cache.put(key, entry);
                }
            }
        }

        countGets++;
        double cacheHitRatio = (double) countCacheHits / (double) countGets;
        double effectiveGets = (double) countFetch / (double) countGets;
        double prefetchRatio = (double) countPrefetch / (double) countGets;
        log.debug("Total gets: " + countGets + ", cache hits: " + countCacheHits + ", fetches: " +
                countFetch+ ", prefetches: " + countPrefetch + ", prefetch hits: " + countPrefetchHits);
        statsF.write(statsPrefix + countGets + "," + countCacheHits + "," + countFetch+ "," +
                countPrefetch + "," + countPrefetchHits);
        statsF.newLine();

        return result;
    }

    @Override
    public Result[] get(List<Get> arg0) throws IOException {
        return htable.get(arg0);
    }

    @Override
    public org.apache.hadoop.hbase.TableName getName() {
        return htable.getName();
    }

    @Override
    public Configuration getConfiguration() {
        return htable.getConfiguration();
    }

    @Override
    public Result getRowOrBefore(byte[] arg0, byte[] arg1) throws IOException {
        return htable.getRowOrBefore(arg0, arg1);
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        // unfold scan into multiple gets
        ResultScanner results = htable.getScanner(scan);

        for(Result result : results) {
            byte[] row = result.getRow();

            Set<byte[]> families = scan.getFamilyMap().keySet();
            for (byte[] f : families) {
                NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(f);
                if (qualifiers != null) {
                    for (byte[] q : qualifiers) {
                        Get get = new Get(row);
                        get.addColumn(f, q);
                        htable.get(get);
                    }
                } else {
                    Get get = new Get(row);
                    get.addFamily(f);
                    htable.get(get);
                }
            }
        }

        return htable.getScanner(scan);
    }

    @Override
    public ResultScanner getScanner(byte[] arg0) throws IOException {
        // TODO implement
        if(1==1) {
            log.fatal("getScanner(byte[]) should not be called!");
            throw new IOException("getScanner(byte[]) should not be called!");
        }
        return htable.getScanner(arg0);
    }

    @Override
    public ResultScanner getScanner(byte[] arg0, byte[] arg1) throws IOException {
        // TODO implement
        if(1==1) {
            log.fatal("getScanner(byte[], byte[]) should not be called!");
            throw new IOException("getScanner(byte[], byte[]) should not be called!");
        }
        return htable.getScanner(arg0, arg1);
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        return htable.getTableDescriptor();
    }

    @Override
    public byte[] getTableName() {
        return htable.getTableName();
    }

    @Override
    public long getWriteBufferSize() {
        return htable.getWriteBufferSize();
    }

    @Override
    public Result increment(Increment arg0) throws IOException {
        return htable.increment(arg0);
    }

    @Override
    public long incrementColumnValue(byte[] arg0, byte[] arg1, byte[] arg2, long arg3) throws IOException {
        return htable.incrementColumnValue(arg0, arg1, arg2, arg3);
    }

    @Override
    public long incrementColumnValue(byte[] bytes, byte[] bytes1, byte[] bytes2, long l, Durability durability) throws IOException {
        return 0;
    }

    @Override
    public long incrementColumnValue(byte[] arg0, byte[] arg1, byte[] arg2, long arg3, boolean arg4) throws IOException {
        return htable.incrementColumnValue(arg0, arg1, arg2, arg3, arg4);
    }

    @Override
    public Boolean[] exists(List<Get> list) throws IOException {
        return new Boolean[0];
    }

    @Override
    public boolean isAutoFlush() {
        return htable.isAutoFlush();
    }

    @Override
    public void mutateRow(RowMutations arg0) throws IOException {
        htable.mutateRow(arg0);
    }

    @Override
    public void put(Put put) throws IOException {
        if(isEnabled && !isMonitoring) {
            CellScanner cellScanner = put.cellScanner();
            while(cellScanner.advance()) {
                Cell c = cellScanner.current();
                String key = DataContainer.getKey(tableName, c);
                cache.put(key, new CacheEntry<>(Result.create(new Cell[]{c})));
                log.debug("Added key to cache: " + key);
            }
        }
        htable.put(put);

//        if (isMonitoring) {
//            long ts = System.currentTimeMillis();
//            Set<byte[]> families = put.getFamilyMap().keySet();
//            for (byte[] f : families) {
//                List<KeyValue> qualifiers = put.getFamilyMap().get(f);
//                for (KeyValue q : qualifiers) {
//                    putOpsF.write(ts + ":" + tableName + ":" + Bytes.toHex(put.getRow()) + ":" + Bytes.toString(f)
//                            + ":" + Bytes.toString(q.getQualifier()));
//                    putOpsF.newLine();
//                }
//            }
//        }
    }

    @Override
    public void put(List<Put> arg0) throws IOException {
        htable.put(arg0);
    }

    @Override
    public void setAutoFlush(boolean arg0) {
        htable.setAutoFlush(arg0);
    }

    @Override
    public void setAutoFlush(boolean arg0, boolean arg1) {
        htable.setAutoFlush(arg0, arg1);
    }

    @Override
    public void setAutoFlushTo(boolean b) {
        htable.setAutoFlushTo(b);
    }

    @Override
    public void setWriteBufferSize(long arg0) throws IOException {
        htable.setWriteBufferSize(arg0);
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes1, R r) throws ServiceException, Throwable {
        return htable.batchCoprocessorService(methodDescriptor, message, bytes, bytes1, r);
    }

    @Override
    public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes1, R r, Callback<R> callback) throws ServiceException, Throwable {
        htable.batchCoprocessorService(methodDescriptor, message, bytes, bytes1, r, callback);
    }

    @Override
    public boolean checkAndMutate(byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, byte[] bytes3, RowMutations rowMutations) throws IOException {
        return htable.checkAndMutate(bytes, bytes1, bytes2, compareOp, bytes3, rowMutations);
    }
}
