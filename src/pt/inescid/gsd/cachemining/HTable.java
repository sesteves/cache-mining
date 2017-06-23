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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class HTable implements HTableInterface {
    private final static String PROPERTIES_FILE = "cachemining.properties";

    private final static String MONITORING_KEY = "monitoring";
    private final static String ENABLED_KEY = "enabled";
    private final static String CACHE_SIZE_KEY = "cache-size";

    private static final String statsFName = String.format("stats-cache-%d.csv", System.currentTimeMillis());

    private static final String STATS_HEADER = "enabled,cachesize,ngets,hits,negets,npfetch,hitpfetch";

    private Logger log = Logger.getLogger(HTable.class);

    private static Map<String, org.apache.hadoop.hbase.client.HTable> htables = new HashMap<String, org.apache.hadoop.hbase.client.HTable>();

    private static Cache<Cell> cache;

    private static SequenceEngine sequenceEngine;

    private org.apache.hadoop.hbase.client.HTable htable;
    private File filePut, fileGet;
    private String tableName;

    private static BufferedWriter statsF;

    private static int countGets = 0, countCacheHits = 0, countEffectiveGets = 0, countPrefetch = 0,
            countPrefetchHits = 0;

    private boolean isMonitoring;
    private boolean isEnabled;

    private String statsPrefix;

    private Lock lockPrefetch = new ReentrantLock();

    private Queue<Get> prefetchQueue = new ConcurrentLinkedQueue<>();

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

    private List<PrefetchingContext> activeContexts = new ArrayList<>();


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
                fileGet = new File("get-operations.log");
            }

            cache = new Cache<>(cacheSize);
            // Would it make sense to run more than 1 thread?
            prefetch.start();

            // TODO create sequence engine without sequences
        }

        statsF = new BufferedWriter(new FileWriter(statsFName));
        statsF.write(STATS_HEADER);
        statsF.newLine();
        statsPrefix = isEnabled + "," + cacheSize + ",";
    }

    public HTable(Configuration conf, String tableName, List<List<DataContainer>> sequences) throws IOException {
        this(conf, tableName);
        sequenceEngine = new SequenceEngine(sequences);
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
        prefetch.stop();
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
        while (true) {

            try {
                prefetchWithContextSemaphore.acquire();
                PrefetchingContext context = prefetchWithContextQueue.poll();

                FetchProgressively iterator = (FetchProgressively) context.getIterator();
                // FIXME
                // FIXME
                // FIXME
                // iterator.unblock();

                List<Get> gets = new ArrayList<>();
                while(iterator.hasNext()) {
                    DataContainer dc = iterator.next();

                    // if data container is already cached, skip it
                    if(cache.contains(dc.toString())) {
                        continue;
                    }

                    Get get = new Get(dc.getRow());
                    if(dc.getQualifier() != null) {
                        get.addColumn(dc.getFamily(), dc.getQualifier());
                    } else {
                        get.addFamily(dc.getFamily());
                    }
                    gets.add(get);

                    context.add(dc);
                    countPrefetch++;
                }

                // prefetching
                Result[] results = htable.get(gets);
                for (Result result : results) {
                    while (result.advance()) {
                        Cell cell = result.current();
                        String key = DataContainer.getKey(tableName, cell);
                        cache.put(key, new CacheEntry<>(cell));
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }



    private List<Cell> fetchFromCache(Get get) {
        List<Cell> result = new ArrayList<>();
        List<byte[]> familiesToRemove = new ArrayList<>();

        for(byte[] family : get.familySet()) {
            NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(family);

            if(qualifiers != null) {

                List<byte[]> qualifiersToRemove = new ArrayList<>();
                for (byte[] qualifier : qualifiers) {
                    // String key = DataContainer.getKey(tableName, get.getRow(), family, qualifier);

                    // CONTEXT
                    DataContainer dc = new DataContainer(getTableName(), get.getRow(), family, qualifier);
                    List<PrefetchingContext> toRemove = new ArrayList<>();
                    for(PrefetchingContext context : activeContexts) {
                        if(context.matches(dc)) {
                            if(context.remove(dc)) {
                                countPrefetchHits++;
                            }

                            // if there is an iterator, it means that we are using progressive fetching
                            if(context.getIterator() != null) {
                                prefetchWithContextQueue.add(context);
                                prefetchWithContextSemaphore.release();
                            }
                        } else {
                            toRemove.add(context);
                        }
                    }
                    activeContexts.removeAll(toRemove);
                    String key = dc.toString();
                    //

                    CacheEntry<Cell> entry = cache.get(key);
                    if (entry != null) {
                        countCacheHits++;
                        result.add(entry.getValue());
                        qualifiersToRemove.add(qualifier);
                    }
                }
                qualifiers.removeAll(qualifiersToRemove);
                if (qualifiers.size() == 0) {
                    familiesToRemove.add(family);
                }

            } else {
                // String key = DataContainer.getKey(tableName, get.getRow(), family);

                // CONTEXT
                DataContainer dc = new DataContainer(getTableName(), get.getRow(), family);
                List<PrefetchingContext> toRemove = new ArrayList<>();
                for(PrefetchingContext context : activeContexts) {
                    if(context.matches(dc)) {
                        if(context.remove(dc)) {
                            countPrefetchHits++;
                        }

                        // if there is an iterator, it means that we are using progressive fetching
                        if(context.getIterator() != null) {
                            prefetchWithContextQueue.add(context);
                            prefetchWithContextSemaphore.release();
                        }
                    } else {
                        toRemove.add(context);
                    }
                }
                activeContexts.removeAll(toRemove);
                String key = dc.toString();
                //

                CacheEntry<Cell> entry = cache.get(key);
                if (entry != null) {
                    countCacheHits++;
                    result.add(entry.getValue());
                    familiesToRemove.add(family);
                }
            }
        }

        for (byte[] family : familiesToRemove) {
            get.getFamilyMap().remove(family);
        }

        return result;
    }

//    private List<KeyValue> getItemsFromCache(Get get, String rowStr) {
//        List<KeyValue> kvs = new ArrayList<>();
//
//        List<byte[]> familiesToRemove = new ArrayList<>();
//        boolean firstItem = true;
//        String key = null;
//
//        for (byte[] family : get.familySet()) {
//            NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(family);
//            if (qualifiers != null) {
//                List<byte[]> qualifiersToRemove = new ArrayList<byte[]>();
//
//                for (byte[] qualifier : qualifiers) {
//                    key = rowStr + SequenceEngine.SEPARATOR + tableName + SequenceEngine.SEPARATOR + Bytes.toString(family)
//                            + SequenceEngine.SEPARATOR + Bytes.toString(qualifier);
//
//                    // System.out.println("Looking up in cache for key '" + key
//                    // + "'");
//                    CacheEntry<List<KeyValue>> entry = cache.get(key);
//                    if (entry != null) {
//                        countCacheHits++;
//
//                        if (firstItem)
//                            doPrefetch = false;
//
//                        // System.out.println("Cache hit: " + countCacheHits);
//                        kvs.addAll(entry.getValue());
//                        qualifiersToRemove.add(qualifier);
//                    }
//                }
//                qualifiers.removeAll(qualifiersToRemove);
//                if (qualifiers.size() == 0)
//                    familiesToRemove.add(family);
//            } else {
//                key = rowStr + SequenceEngine.SEPARATOR + tableName + SequenceEngine.SEPARATOR + Bytes.toString(family);
//
//                // System.out.println("Looking up in cache for key '" + key +
//                // "'");
//                CacheEntry<List<KeyValue>> entry = cache.get(key);
//                if (entry != null) {
//                    countCacheHits++;
//
//                    if (firstItem)
//                        doPrefetch = false;
//                    // System.out.println("Cache hit: " + countCacheHits);
//                    kvs.addAll(entry.getValue());
//                    familiesToRemove.add(family);
//                }
//            }
//            firstItem = false;
//        }
//
//        for (byte[] family : familiesToRemove)
//            get.getFamilyMap().remove(family);
//
//        return kvs;
//    }

    private void prefetch() {
//        private void prefetch(Get get, String rowStr, String firstItem) throws IOException {

        while(true) {

            try {
                prefetchSemaphore.acquire();
                Get get = prefetchQueue.poll();

                long startTick = System.currentTimeMillis();

                Map.Entry<byte[], NavigableSet<byte[]>> e = get.getFamilyMap().entrySet().iterator().next();
                DataContainer firstItem = new DataContainer(getTableName(), get.getRow(), e.getKey(),
                        e.getValue().iterator().next());

                System.out.println("First item: " + firstItem);

                // get sequences matching firstItem
                Heuristic itemsIt = sequenceEngine.getSequences(firstItem);
                if (itemsIt == null) {
                    log.debug("There is no sequence indexed by key '" + firstItem + "'.");
                    continue;
                }
                log.debug("There are sequences indexed by key '" + firstItem + "'.");

                // creates prefetching context
                PrefetchingContext context = new PrefetchingContext(itemsIt);

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
                    if(cache.contains(item.toString())) {
                        continue;
                    }

                    List<Get> tableGets = gets.get(item.getTableStr());
                    if (tableGets == null) {
                        tableGets = new ArrayList<>();
                        gets.put(item.getTableStr(), tableGets);
                    }
                    Get g = new Get(item.getRow());
                    if(item.getQualifier() == null) {
                        g.addFamily(item.getFamily());
                    } else {
                        g.addColumn(item.getFamily(), item.getQualifier());
                    }
                    tableGets.add(g);

                    countPrefetch++;
                    context.add(item);
                }

                // if elements were prefetched
                if(context.getCount() > 0) {
                    context.setContainersPerLevel(((Heuristic) itemsIt).getContainersPerLevel());
                    activeContexts.add(context);
                }

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

            } catch (Exception e) {
                e.printStackTrace();
            }
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


    private void monitorGet(Get get, String rowStr) throws IOException {
        FileWriter fw = new FileWriter(fileGet.getAbsoluteFile(), true);
        BufferedWriter bw = new BufferedWriter(fw);

        long ts = System.currentTimeMillis();
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
        log.info("get CALLED (TABLE: " + tableName + ", ROW: " + Bytes.toString(get.getRow()) + ", COLUMNS: "
                + getColumnsStr(get.getFamilyMap()) + ")");

        if (!isEnabled) {
            return htable.get(get);
        }

        final String rowStr = bytesToStr(get.getRow());
        if (isMonitoring) {
            monitorGet(get, rowStr);
            return htable.get(get);
        }

        // fetch items from cache
        // TODO send rowStr so that it does not need to be converted again
        List<Cell> result = fetchFromCache(get);


        // FIXME: if there is cache hit, then nothing is prefetched
        if(get.hasFamilies()) {
            // prefetch sequences in the background asynchronously
            prefetchQueue.add(get);
            prefetchSemaphore.release();

            countEffectiveGets++;
            Result partialResult = htable.get(get);
            // add fetched result to cache
            while(partialResult.advance()) {
                Cell cell = partialResult.current();
                String key = DataContainer.getKey(tableName, cell);
                cache.put(key, new CacheEntry<>(cell));
            }

            // TODO check if it is necessary to sort cells
            result.addAll(partialResult.listCells());
        }

        countGets++;
        double cacheHitRatio = (double) countCacheHits / (double) countGets;
        double effectiveGets = (double) countEffectiveGets / (double) countGets;
        double prefetchRatio = (double) countPrefetch / (double) countGets;
        log.debug("Total gets: " + countGets + ", Cache hit ratio: " + cacheHitRatio + ", Effective gets: " +
                effectiveGets + ", Prefetch ratio: " + prefetchRatio);
        statsF.write(statsPrefix + countGets + "," + countCacheHits + "," + countEffectiveGets + "," +
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
    public ResultScanner getScanner(Scan arg0) throws IOException {
        return htable.getScanner(arg0);
    }

    @Override
    public ResultScanner getScanner(byte[] arg0) throws IOException {
        return htable.getScanner(arg0);
    }

    @Override
    public ResultScanner getScanner(byte[] arg0, byte[] arg1) throws IOException {
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
