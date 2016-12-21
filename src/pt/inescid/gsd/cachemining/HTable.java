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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class HTable implements HTableInterface {

    private final static String PROPERTIES_FILE = "cachemining.properties";

    private final static String MONITORING_KEY = "monitoring";
    private final static String ENABLED_KEY = "enabled";
    private final static String MONITORING_DEFAULT = "false";
    private final static String ENABLED_DEFAULT = "false";

    private Logger log = Logger.getLogger(HTable.class);

    private static Map<String, org.apache.hadoop.hbase.client.HTable> htables = new HashMap<String, org.apache.hadoop.hbase.client.HTable>();

    private static Cache<Cell> cache = new Cache<>();

    private static SequenceEngine sequenceEngine = new SequenceEngine();

    private org.apache.hadoop.hbase.client.HTable htable;
    private File filePut, fileGet;
    private String tableName;

    private static int countGets = 0, countCacheHits = 0, countEffectiveGets = 0, countPrefetch = 0;

    private boolean isMonitoring;
    private boolean isEnabled;

    private boolean doPrefetch;

    private Lock lockPrefetch = new ReentrantLock();

    private Thread prefetch = new Thread(new Runnable() {
        @Override
        public void run() {
            prefetch();
        }
    });

    public HTable(Configuration conf, String tableName) throws IOException {
        PropertyConfigurator.configure("cachemining-log4j.properties");

        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(PROPERTIES_FILE));
        } catch (IOException e) {
            log.info("Could not load properties file '" + PROPERTIES_FILE + "'.");
        }
        isMonitoring = Boolean.parseBoolean(properties.getProperty(MONITORING_KEY, MONITORING_DEFAULT));
        isEnabled = Boolean.parseBoolean(properties.getProperty(ENABLED_KEY, ENABLED_DEFAULT));

        log.info("HTable (Enabled: " + isEnabled + ")");

        // TODO delete files if they exist
        filePut = new File("put-operations.log");
        fileGet = new File("get-operations.log");
        this.tableName = tableName;
        htable = new org.apache.hadoop.hbase.client.HTable(conf, tableName);
        htables.put(tableName, htable);

        // Would it make sense to run more than 1 thread?
        prefetch.run();
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
        htable.close();

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

    private List<Cell> fetchFromCache(Get get) {
        List<Cell> result = new ArrayList<>();
        List<byte[]> familiesToRemove = new ArrayList<>();

        String rowStr = "";

        for(byte[] family : get.familySet()) {
            NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(family);

            String key = rowStr + SequenceEngine.SEPARATOR + tableName + SequenceEngine.SEPARATOR + Bytes.toString(family);

            if(qualifiers != null) {

                List<byte[]> qualifiersToRemove = new ArrayList<>();
                for (byte[] qualifier : qualifiers) {

                    String finalKey = key + SequenceEngine.SEPARATOR + Bytes.toString(qualifier);
                    CacheEntry<Cell> entry = cache.get(finalKey);
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


            lockPrefetch.lock();

            long startTick = System.currentTimeMillis();

            try {

                // get sequences matching firstItem
                Set<String> sequence = sequenceEngine.getSequence(firstItem);
                if (sequence == null) {
                    log.debug("There is no sequence indexed by key '" + firstItem + "'.");
                    return;
                }

                log.debug("There are sequences indexed by key '" + firstItem + "'.");

                // TODO return elements of sequence already batched ?
                // batch updates to the same tables
                Map<String, Get> gets = new HashMap<>();
                for (String item : sequence) {

                    String[] elements = item.split(SequenceEngine.SEPARATOR);
                    String tableName = elements[0];
                    String row = elements[1];
                    String family = elements[2];
                    String qualifier = elements.length == 4 ? elements[3] : "";

                    String key = tableName + SequenceEngine.SEPARATOR + row + SequenceEngine.SEPARATOR + family;
                    if (!"".equals(qualifier)) {
                        key += SequenceEngine.SEPARATOR + qualifier;
                    }

                    // if current item is part of the get request or item is in cache, skip it
                    if ((this.tableName == tableName && get.getFamilyMap().containsKey(family) && get.getFamilyMap().get(family)
                            .contains(qualifier)) || cache.contains(key)) {
                        continue;
                    }

                    Get g = gets.get(tableName);
                    if (g == null) {
                        g = new Get(strToBytes(row));
                        gets.put(tableName, g);
                    }
                    g.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                }


                // pre-fetch elements to cache
                for (Map.Entry<String, Get> entry : gets.entrySet()) {
                    Result result = htables.get(entry.getKey()).get(entry.getValue());

                    if (result.current() != null) {
                        System.out.println("#### CURRENT IS NOT NULL!!!!");
                    }

                    while (result.advance()) {
                        Cell cell = result.current();

                        String key = rowStr + SequenceEngine.SEPARATOR + tableName + SequenceEngine.SEPARATOR
                                + Bytes.toString(cell.getFamily()) + SequenceEngine.SEPARATOR + Bytes.toString(cell.getQualifier());

                        cache.put(key, new CacheEntry<>(cell));
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        long diff = System.currentTimeMillis() - startTick;
        log.debug("Time taken with prefetching: " + diff);
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
        List<Cell> result = fetchFromCache(get);


        if(get.hasFamilies()) {
            // prefetch sequences in the background

            lockPrefetch.unlock();

            countEffectiveGets++;
            Result partialResult = htable.get(get);
            // TODO check if it is necessary to sort cells
            result.addAll(partialResult.listCells());
        }

        countGets++;
        double cacheHitRate = (double) countCacheHits / (double) countGets;
        double effectiveGets = (double) countEffectiveGets / (double) countGets;
        double prefetchRatio = (double) countPrefetch / (double) countGets;
        log.debug("Total gets: " + countGets + ", Cache hit rate: " + cacheHitRate + ", Effective gets: " + effectiveGets
                + ", Prefetch ratio: " + prefetchRatio);

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
