package pt.inescid.gsd.cachemining;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class HTable implements HTableInterface {

    private final static String PROPERTIES_FILE = "cachemining.properties";

    private final static String MONITORING_KEY = "monitoring";
    private final static String ENABLED_KEY = "enabled";
    private final static String MONITORING_DEFAULT = "false";
    private final static String ENABLED_DEFAULT = "false";

    private Logger log = Logger.getLogger(HTable.class);

    private static Map<String, org.apache.hadoop.hbase.client.HTable> htables = new HashMap<String, org.apache.hadoop.hbase.client.HTable>();

    private static Cache<List<KeyValue>> cache = new Cache<List<KeyValue>>();

    private static SequenceEngine sequenceEngine = new SequenceEngine();

    private org.apache.hadoop.hbase.client.HTable htable;
    private File filePut, fileGet;
    private String tableName;

    private static int countGets = 0, countCacheHits = 0, countEffectiveGets = 0, countPrefetch = 0;

    private boolean isMonitoring;
    private boolean isEnabled;

    private boolean doPrefetch;

    public HTable(Configuration conf, String tableName) throws IOException {
        PropertyConfigurator.configure("cachemining-log4j.properties");

        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(PROPERTIES_FILE));
        } catch (IOException e) {
            log.info("Not possible to load properties file '" + PROPERTIES_FILE + "'.");
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
    public void batch(List<? extends Row> arg0, Object[] arg1) throws IOException, InterruptedException {
        htable.batch(arg0, arg1);
    }

    @Override
    public boolean checkAndDelete(byte[] arg0, byte[] arg1, byte[] arg2, byte[] arg3, Delete arg4) throws IOException {
        return htable.checkAndDelete(arg0, arg1, arg2, arg3, arg4);
    }

    @Override
    public boolean checkAndPut(byte[] arg0, byte[] arg1, byte[] arg2, byte[] arg3, Put arg4) throws IOException {
        return htable.checkAndPut(arg0, arg1, arg2, arg3, arg4);
    }

    @Override
    public void close() throws IOException {
        htable.close();

    }

    @Override
    public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(Class<T> arg0, byte[] arg1, byte[] arg2,
            Call<T, R> arg3) throws IOException, Throwable {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends CoprocessorProtocol, R> void coprocessorExec(Class<T> arg0, byte[] arg1, byte[] arg2, Call<T, R> arg3,
            Callback<R> arg4) throws IOException, Throwable {
        // TODO Auto-generated method stub

    }

    @Override
    public <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> arg0, byte[] arg1) {
        // TODO Auto-generated method stub
        return null;
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

    private List<KeyValue> getItemsFromCache(Get get, String rowStr) {
        List<KeyValue> kvs = new ArrayList<KeyValue>();

        List<byte[]> familiesToRemove = new ArrayList<byte[]>();
        boolean firstItem = true;
        String key = null;
        for (byte[] family : get.familySet()) {
            NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(family);
            if (qualifiers != null) {
                List<byte[]> qualifiersToRemove = new ArrayList<byte[]>();

                for (byte[] qualifier : qualifiers) {
                    key = rowStr + SequenceEngine.SEPARATOR + tableName + SequenceEngine.SEPARATOR + Bytes.toString(family)
                            + SequenceEngine.SEPARATOR + Bytes.toString(qualifier);

                    // System.out.println("Looking up in cache for key '" + key
                    // + "'");
                    CacheEntry<List<KeyValue>> entry = cache.get(key);
                    if (entry != null) {
                        countCacheHits++;

                        if (firstItem)
                            doPrefetch = false;

                        // System.out.println("Cache hit: " + countCacheHits);
                        kvs.addAll(entry.getValue());
                        qualifiersToRemove.add(qualifier);
                    }
                }
                qualifiers.removeAll(qualifiersToRemove);
                if (qualifiers.size() == 0)
                    familiesToRemove.add(family);
            } else {
                key = rowStr + SequenceEngine.SEPARATOR + tableName + SequenceEngine.SEPARATOR + Bytes.toString(family);

                // System.out.println("Looking up in cache for key '" + key +
                // "'");
                CacheEntry<List<KeyValue>> entry = cache.get(key);
                if (entry != null) {
                    countCacheHits++;

                    if (firstItem)
                        doPrefetch = false;
                    // System.out.println("Cache hit: " + countCacheHits);
                    kvs.addAll(entry.getValue());
                    familiesToRemove.add(family);
                }
            }
            firstItem = false;
        }
        for (byte[] family : familiesToRemove)
            get.getFamilyMap().remove(family);

        return kvs;
    }

    private void prefetch(Get get, String rowStr, String firstItem) throws IOException {
        long startTick = System.currentTimeMillis();

        Set<String> sequence = sequenceEngine.getSequence(firstItem);
        if (sequence == null) {
            // System.out.println("### There is no sequence indexed by key '" +
            // firstItem + "'.");
            return;
        }

        // System.out.println("### There are sequences indexed by key '" +
        // firstItem + "'.");
        Map<String, Get> gets = new HashMap<String, Get>();

        // FIXME return elements of sequence already batched ?
        // batch updates to the same tables
        for (String item : sequence) {

            String[] elements = item.split(":");
            String tableName = elements[0];
            String family = elements[1];
            String qualifier = elements.length == 3 ? elements[2] : "";

            String key = rowStr + SequenceEngine.SEPARATOR + tableName + SequenceEngine.SEPARATOR + family
                    + SequenceEngine.SEPARATOR + qualifier;

            if ((this.tableName == tableName && get.getFamilyMap().containsKey(family) && get.getFamilyMap().get(family)
                    .contains(qualifier))
                    || cache.contains(key))
                continue;

            Get g = gets.get(tableName);
            if (g == null)
                g = new Get(get.getRow());
            g.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            gets.put(tableName, g);

            // debugging purposes
            // StringBuilder sb = new StringBuilder();
            // for (byte b : g.getRow())
            // sb.append(String.format("\\x%02x", b & 0xFF));
            // System.out.println("### Item from sequence tableName: " +
            // tableName + ", row: " + Bytes.toString(get.getRow()) + " - "
            // + sb.toString() + "-, family: " + family + ", qualifier: " +
            // qualifier);
        }

        // pre-fetch elements to cache
        for (String tableName : gets.keySet()) {
            Result result = htables.get(tableName).get(gets.get(tableName));
            if (result.isEmpty())
                continue;

            for (KeyValue kv : result.raw()) {

                // System.out.println("### Putting in cache KeyValue with key: "
                // + kv.getKeyString() + ", tableName: " + tableName
                // + ", Family: " + Bytes.toString(kv.getFamily()) +
                // ", Qualifier: " + Bytes.toString(kv.getQualifier())
                // + ", Value: " + Bytes.toString(kv.getValue()));

                String key = rowStr + SequenceEngine.SEPARATOR + tableName + SequenceEngine.SEPARATOR
                        + Bytes.toString(kv.getFamily()) + SequenceEngine.SEPARATOR + Bytes.toString(kv.getQualifier());
                // FIXME cache entry should correspond to a single KeyValue ?
                List<KeyValue> kvs = new ArrayList<KeyValue>();
                kvs.add(kv);
                cache.put(key, new CacheEntry<List<KeyValue>>(kvs));
            }

        }
        // System.out.println("### Cache contents: " + cache);

        long diff = System.currentTimeMillis() - startTick;
        log.debug("Time taken with prefetching: " + diff);
    }

    @Override
    public Result get(Get get) throws IOException {
        log.info("get CALLED (TABLE: " + tableName + ", ROW: " + Bytes.toString(get.getRow()) + ", COLUMNS: "
                + getColumnsStr(get.getFamilyMap()) + ")");

        if (!isEnabled)
            return htable.get(get);

        StringBuilder sb = new StringBuilder();
        for (byte b : get.getRow())
            sb.append(String.format("\\x%02x", b & 0xFF));
        final String rowStr = sb.toString();

        if (isMonitoring) {
            Result result = htable.get(get);
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
            return result;
        }

        countGets++;
        doPrefetch = true;

        byte[] family = get.familySet().iterator().next();
        sb = new StringBuilder(tableName + SequenceEngine.SEPARATOR + Bytes.toString(family));
        if (get.getFamilyMap().get(family) != null) {
            String qualifier = Bytes.toString(get.getFamilyMap().get(family).iterator().next());
            sb.append(SequenceEngine.SEPARATOR + qualifier);
        }
        final String firstItem = sb.toString();

        // fetch items from cache
        List<KeyValue> kvs = getItemsFromCache(get, rowStr);

        if (get.hasFamilies()) {

            countEffectiveGets++;

            // prefetch
            if (doPrefetch) {
                countPrefetch++;

                final Get finalGet = get;
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            prefetch(finalGet, rowStr, firstItem);
                        } catch (IOException e) {
                            log.fatal(e.getMessage());
                        }
                    }
                }).run();

            }

            // get remaining items
            Result partialResult = htable.get(get);

            // merge results
            if (!partialResult.isEmpty()) {
                kvs.addAll(partialResult.list());
                Collections.sort(kvs, KeyValue.COMPARATOR);
            }
        }

        double cacheHitRate = (double) countCacheHits / (double) countGets;
        double effectiveGets = (double) countEffectiveGets / (double) countGets;
        double prefetchRatio = (double) countPrefetch / (double) countGets;
        log.debug("Total gets: " + countGets + ", Cache hit rate: " + cacheHitRate + ", Effective gets: " + effectiveGets
                + ", Prefetch ratio: " + prefetchRatio);

        return new Result(kvs);
    }

    @Override
    public Result[] get(List<Get> arg0) throws IOException {
        return htable.get(arg0);
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
    public long incrementColumnValue(byte[] arg0, byte[] arg1, byte[] arg2, long arg3, boolean arg4) throws IOException {
        return htable.incrementColumnValue(arg0, arg1, arg2, arg3, arg4);
    }

    @Override
    public boolean isAutoFlush() {
        return htable.isAutoFlush();
    }

    @Override
    public RowLock lockRow(byte[] arg0) throws IOException {
        return htable.lockRow(arg0);
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
    public void setWriteBufferSize(long arg0) throws IOException {
        htable.setWriteBufferSize(arg0);
    }

    @Override
    public void unlockRow(RowLock arg0) throws IOException {
        htable.unlockRow(arg0);
    }

}
