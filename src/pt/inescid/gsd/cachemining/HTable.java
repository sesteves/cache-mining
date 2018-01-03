package pt.inescid.gsd.cachemining;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class HTable implements HTableInterface {

    private final static int NUMBER_OF_THREADS = 4;

    private final static String PROPERTIES_FILE = "cachemining.properties";

    private final static String MONITORING_KEY = "monitoring";
    private final static String ENABLED_KEY = "enabled";
    private final static String CACHE_SIZE_KEY = "cache-size";
    private final static String HEURISTIC_KEY = "heuristic";

    private static final String statsFName = String.format("stats-cache-%d.csv", System.currentTimeMillis());

    private static final String STATS_HEADER = "cachesize,ngets,hits,negets,npfetch,hitpfetch";

    private Logger log = Logger.getLogger(HTable.class);

    private static Map<String, org.apache.hadoop.hbase.client.HTable> htables = new HashMap<String, org.apache.hadoop.hbase.client.HTable>();

    private static Cache<Cell> cache;

    private static SequenceEngine sequenceEngine;

    private org.apache.hadoop.hbase.client.HTable htable;

    private static File filePut, fileGet;

    private String tableName;

    private static BufferedWriter statsF;

    private static int countGets = 0, countCacheHits = 0, countFetch = 0, countPrefetch = 0,
            countPrefetchHits = 0;

    private boolean isMonitoring;
    private boolean isEnabled;

    private String statsPrefix;

    private Queue<DataContainer> prefetchQueue = new ConcurrentLinkedQueue<>();

    private Queue<PrefetchingContext> prefetchWithContextQueue = new ConcurrentLinkedQueue<>();

    private final Semaphore prefetchSemaphore = new Semaphore(0);

    private final Semaphore prefetchWithContextSemaphore = new Semaphore(0);

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

    ExecutorService executorPrefetch, executorPrefetchWithContext;

    private List<PrefetchingContext> activeContexts = new ArrayList<>();

    private Object activeContextsLock = new Object();

    public HTable(Configuration conf, String tableName) throws IOException {
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
                // TODO delete files if they exist
                filePut = new File("put-operations.log");
                fileGet = new File(String.format("get-ops-%d.txt", System.currentTimeMillis()));
            }

            cache = new Cache<>(cacheSize);

            executorPrefetch = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
            executorPrefetchWithContext = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
            for(int i = 0; i < NUMBER_OF_THREADS; i++) {
                executorPrefetch.execute(prefetch);
                executorPrefetchWithContext.execute(prefetchWithContext);
            }

            // TODO create sequence engine without sequences
        }

        statsF = new BufferedWriter(new FileWriter(statsFName));
        statsF.write(STATS_HEADER);
        statsF.newLine();
        statsPrefix = cacheSize + ",";

    }

    public HTable(Configuration conf, String tableName, List<List<DataContainer>> sequences) throws IOException {
        this(conf, tableName);
        if (isEnabled) {
            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream(PROPERTIES_FILE));
            } catch (IOException e) {
                log.info("Could not load properties file '" + PROPERTIES_FILE + "'.");
            }

            String heuristic = System.getProperty(HEURISTIC_KEY, properties.getProperty(HEURISTIC_KEY));
            sequenceEngine = new SequenceEngine(sequences, heuristic);
        }
    }

    public void markTransaction() throws IOException {
        if (!(isEnabled && isMonitoring))
            return;

        FileWriter fw = new FileWriter(fileGet.getAbsoluteFile(), true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write("### TRANSACTION " + System.currentTimeMillis());
        bw.newLine();
        bw.close();
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
        statsF.close();
        htable.close();
        if (isEnabled) {
            executorPrefetch.shutdownNow();
            executorPrefetchWithContext.shutdownNow();
        }
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
        String columns = "";
        Set<byte[]> families = familyMap.keySet();
        for (byte[] family : families) {
            columns += ", " + Bytes.toString(family);
            NavigableSet<byte[]> qualifiers = familyMap.get(family);
            if (qualifiers != null)
                for (byte[] qualifier : qualifiers)
                    columns += ":" + Bytes.toString(qualifier);
        }
        return columns.substring(1);
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
                    while (result.advance()) {
                        Cell cell = result.current();
                        String key = DataContainer.getKey(tableName, cell);
                        cache.put(key, new CacheEntry<>(cell));
                    }
                }
            }
        } catch (InterruptedException e) {
            log.debug(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


//    private List<Cell> fetchFromCache(Get get) {
//        List<Cell> result = new ArrayList<>();
//        // List<byte[]> familiesToRemove = new ArrayList<>();
//
//        for(byte[] family : get.familySet()) {
//            NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(family);
//
//            if(qualifiers != null) {
//
//                // List<byte[]> qualifiersToRemove = new ArrayList<>();
//                for (byte[] qualifier : qualifiers) {
//                    // String key = DataContainer.getKey(tableName, get.getRow(), family, qualifier);
//
//
//                    // CONTEXT
//                    boolean prefetchHit = false;
//                    DataContainer dc = new DataContainer(getTableName(), get.getRow(), family, qualifier);
//                    List<PrefetchingContext> toRemove = new ArrayList<>();
//
//                    synchronized (activeContextsLock) {
//                        for (PrefetchingContext context : activeContexts) {
//                            if (context.matches(dc)) {
//                                if (context.remove(dc)) {
//                                    countPrefetchHits++;
//                                    prefetchHit = true;
//                                }
//
//                                // if there is an iterator, it means that we are using progressive fetching
//                                if (context.getIterator() != null) {
//                                    context.setLastRequestedDc(dc);
//                                    prefetchWithContextQueue.add(context);
//                                    prefetchWithContextSemaphore.release();
//                                }
//                            } else {
//                                toRemove.add(context);
//                            }
//                        }
//                        activeContexts.removeAll(toRemove);
//                    }
//                    String key = dc.toString();
//
//                    // if there is a prefetch hit, then wait until element is in cache
//                    CacheEntry<Cell> entry;
//                    do {
//                        entry = cache.get(key);
//                    } while(prefetchHit && entry == null);
//
//                    if (entry != null) {
//                        countCacheHits++;
//                        result.add(entry.getValue());
////                        qualifiersToRemove.add(qualifier);
//                    }
//                }
////                qualifiers.removeAll(qualifiersToRemove);
////                if (qualifiers.size() == 0) {
////                    familiesToRemove.add(family);
////                }
//
//            } else {
//                // String key = DataContainer.getKey(tableName, get.getRow(), family);
//
//                // CONTEXT
//                DataContainer dc = new DataContainer(getTableName(), get.getRow(), family);
//                List<PrefetchingContext> toRemove = new ArrayList<>();
//                for(PrefetchingContext context : activeContexts) {
//                    if(context.matches(dc)) {
//                        if(context.remove(dc)) {
//                            countPrefetchHits++;
//                        }
//
//                        // if there is an iterator, it means that we are using progressive fetching
//                        if(context.getIterator() != null) {
//                            prefetchWithContextQueue.add(context);
//                            prefetchWithContextSemaphore.release();
//                        }
//                    } else {
//                        toRemove.add(context);
//                    }
//                }
//                activeContexts.removeAll(toRemove);
//                String key = dc.toString();
//                //
//
//                CacheEntry<Cell> entry = cache.get(key);
//                if (entry != null) {
//                    countCacheHits++;
//                    result.add(entry.getValue());
//  //                  familiesToRemove.add(family);
//                }
//            }
//        }
//
////        for (byte[] family : familiesToRemove) {
////            get.getFamilyMap().remove(family);
////        }
//
//        return result;
//    }

    private List<Cell> fetchFromCache(DataContainer dc) {
        List<Cell> result = new ArrayList<>();

        // CONTEXT
        boolean prefetchHit = false;
        List<PrefetchingContext> toRemove = new ArrayList<>();

        synchronized (activeContextsLock) {
            for (PrefetchingContext context : activeContexts) {
                if (context.matches(dc)) {
                    if (context.remove(dc)) {
                        countPrefetchHits++;
                        prefetchHit = true;
                    }

                    // if there is an iterator, it means that we are using progressive fetching
                    if (context.getIterator() != null) {
                        context.setLastRequestedDc(dc);
                        prefetchWithContextQueue.add(context);
                        prefetchWithContextSemaphore.release();
                    }
                } else {
                    toRemove.add(context);
                }
            }
            activeContexts.removeAll(toRemove);
        }
        String key = dc.toString();

        // if there is a prefetch hit, then wait until element is in cache
        CacheEntry<Cell> entry;
        do {
            entry = cache.get(key);
        } while (prefetchHit && entry == null);
        if (entry != null) {
            countCacheHits++;
            result.add(entry.getValue());
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

                // creates prefetching context
                PrefetchingContext context = new PrefetchingContext(itemsIt);
                synchronized (activeContextsLock) {
                    activeContexts.add(context);
                }
                // context.setContainersPerLevel(itemsIt.getContainersPerLevel());

                // batch updates to the same tables
                Map<String, List<Get>> gets = new HashMap<>();
                while (itemsIt.hasNext()) {
                    DataContainer item = itemsIt.next();

                    // TODO make sure current item is not part of get request - sequencing engine does not return 1st item
                    // if either current item is part of the get request or item is in cache, skip it
//                    if ((tableName.equals(item.getTableStr()) && get.getFamilyMap().containsKey(item.getFamily()) &&
//                            get.getFamilyMap().get(item.getFamily()).contains(item.getQualifier())) ||
//                            cache.contains(item.toString())) {
//                        continue;
//                    }
                    if (cache.contains(item.toString())) {
                        continue;
                    }

                    List<Get> tableGets = gets.get(item.getTableStr());
                    if (tableGets == null) {
                        tableGets = new ArrayList<>();
                        gets.put(item.getTableStr(), tableGets);
                    }
                    Get g = new Get(item.getRow());
                    if (item.getQualifier() == null) {
                        g.addFamily(item.getFamily());
                    } else {
                        g.addColumn(item.getFamily(), item.getQualifier());
                    }
                    tableGets.add(g);

                    countPrefetch++;
                    context.add(item);
                }

//                if(context.getCount() == 0) {
//                    activeContexts.remove(context);
//                }

                // TODO this scheme disrupts the order by which items are retrieved from the iterator
                // prefetch elements to cache
                for (Map.Entry<String, List<Get>> entry : gets.entrySet()) {
                    String tableName = entry.getKey();
                    Result[] results = htables.get(tableName).get(entry.getValue());

                    for (Result result : results) {
                        while (result.advance()) {
                            Cell cell = result.current();
                            String key = DataContainer.getKey(entry.getKey(), cell);
                            cache.put(key, new CacheEntry<>(cell));
                        }
                    }
                }

                long diff = System.currentTimeMillis() - startTick;
                log.debug("Time taken with prefetching: " + diff);

            }
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
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
        FileWriter fw = new FileWriter(fileGet.getAbsoluteFile(), true);
        BufferedWriter bw = new BufferedWriter(fw);

        long ts = System.currentTimeMillis();
        String rowStr = "" + Bytes.toInt(get.getRow());
        Set<byte[]> families = get.familySet();
        for (byte[] f : families) {
            NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(f);
            if (qualifiers != null) {
                for (byte[] q : qualifiers) {
                    bw.write(ts + ":" + tableName + ":" + rowStr + ":" + Bytes.toString(f) + ":" + Bytes.toString(q));
                    bw.newLine();
                }
            } else {
                bw.write(ts + ":" + tableName + ":" + rowStr + ":" + Bytes.toString(f));
                bw.newLine();
            }
        }
        bw.close();
    }

    @Override
    public Result get(Get get) throws IOException {
        log.debug("get CALLED (" + tableName + ":" + Bytes.toString(get.getRow()) + ":"
                + getColumnsStr(get.getFamilyMap()) + ")");

        if (!isEnabled) {
            return htable.get(get);
        }
        if (isMonitoring) {
            monitorGet(get);
            return htable.get(get);
        }

        Map.Entry<byte[], NavigableSet<byte[]>> e = get.getFamilyMap().entrySet().iterator().next();
        DataContainer dc = new DataContainer(getTableName(), get.getRow(), e.getKey(), e.getValue().iterator().next());

        // fetch items from cache
        List<Cell> result = fetchFromCache(dc);

        if(result.isEmpty()) {
            // prefetch sequences in the background asynchronously
            prefetchQueue.add(dc);
            prefetchSemaphore.release();

            countFetch++;
            Result partialResult = htable.get(get);
            // add fetched result to cache
            while(partialResult.advance()) {
                Cell cell = partialResult.current();
                String key = DataContainer.getKey(tableName, cell);
                cache.put(key, new CacheEntry<>(cell));
            }

            result.addAll(partialResult.listCells());
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

        return new Result().create(result);
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
        // FIXME
        if(1==1)
            throw new IOException("getScanner(byte[]) should not be called!");
        return htable.getScanner(arg0);
    }

    @Override
    public ResultScanner getScanner(byte[] arg0, byte[] arg1) throws IOException {
        // FIXME
        if(1==1)
            throw new IOException("getScanner(byte[], byte[]) should not be called!");
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
        htable.put(put);
        if (isMonitoring) {
            FileWriter fw = new FileWriter(filePut.getAbsoluteFile(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            long ts = System.currentTimeMillis();
            Set<byte[]> families = put.getFamilyMap().keySet();
            for (byte[] f : families) {
                List<KeyValue> qualifiers = put.getFamilyMap().get(f);
                for (KeyValue q : qualifiers) {
                    bw.write(ts + ":" + tableName + ":" + Bytes.toInt(put.getRow()) + ":" + Bytes.toString(f) + ":"
                            + Bytes.toString(q.getQualifier()));
                    bw.newLine();
                }
            }
            bw.close();
        }
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
