package pt.inescid.gsd.cachemining;

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

    private static Cache<Result> cache = new Cache<Result>();

    private static SequenceEngine sequenceEngine = new SequenceEngine();

    private org.apache.hadoop.hbase.client.HTable htable;
    private File filePut, fileGet;
    private String tableName;

    private static int countGets = 0, countCacheHits = 0;

    private boolean isMonitoring;
    private boolean isEnabled;

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

        filePut = new File("put-operations.log");
        fileGet = new File("get-operations.log");
        this.tableName = tableName;
        htable = new org.apache.hadoop.hbase.client.HTable(conf, tableName);
        htables.put(tableName, htable);
    }

    public void setScannerCaching(int scannerCaching) {
        htable.setScannerCaching(scannerCaching);
    }

    @Override
    public Result append(Append arg0) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object[] batch(List<? extends Row> arg0) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void batch(List<? extends Row> arg0, Object[] arg1) throws IOException, InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean checkAndDelete(byte[] arg0, byte[] arg1, byte[] arg2, byte[] arg3, Delete arg4) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean checkAndPut(byte[] arg0, byte[] arg1, byte[] arg2, byte[] arg3, Put arg4) throws IOException {
        // TODO Auto-generated method stub
        return false;
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
        // TODO Auto-generated method stub
        return false;
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

    private List<String> getItemsFromGet(Get get) {
        List<String> items = new ArrayList<String>();

        final String tableName = this.tableName;
        final String row = Bytes.toString(get.getRow());
        String colFamily = null;
        String colQualifier = null;
        Set<byte[]> families = get.familySet();
        for (byte[] family : families) {
            colFamily = Bytes.toString(family);
            NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(family);
            if (qualifiers != null) {
                for (byte[] qualifier : qualifiers) {
                    colQualifier = Bytes.toString(qualifier);

                    String key = tableName + SequenceEngine.SEPARATOR + colFamily + SequenceEngine.SEPARATOR + colQualifier;
                    items.add(key);
                }
            } else {
                String key = tableName + SequenceEngine.SEPARATOR + colFamily;
                items.add(key);
            }
        }

        return items;
    }

    @Override
    public Result get(Get get) throws IOException {
        log.info("get CALLED (TABLE: " + tableName + ", ROW: " + Bytes.toInt(get.getRow()) + ", COLUMNS: "
                + getColumnsStr(get.getFamilyMap()) + ")");
        System.out.println("get CALLED (TABLE: " + tableName + ", ROW: " + Bytes.toInt(get.getRow()) + ", COLUMNS: "
                + getColumnsStr(get.getFamilyMap()) + ")");

        Result result = null;
        if (isMonitoring) {
            result = htable.get(get);
            FileWriter fw = new FileWriter(fileGet.getAbsoluteFile(), true);
            BufferedWriter bw = new BufferedWriter(fw);

            long ts = System.currentTimeMillis();
            Set<byte[]> families = get.familySet();
            for (byte[] f : families) {
                NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(f);
                if (qualifiers != null) {
                    for (byte[] q : qualifiers) {
                        bw.write(ts + ":" + tableName + ":" + Bytes.toInt(get.getRow()) + ":" + Bytes.toString(f) + ":"
                                + Bytes.toString(q));
                        bw.newLine();
                    }
                } else {
                    bw.write(ts + ":" + tableName + ":" + Bytes.toInt(get.getRow()) + ":" + Bytes.toString(f));
                    bw.newLine();
                }
            }
            bw.close();
            return result;
        }

        result = new Result();

        // decomposition of items to get
        List<String> getItems = getItemsFromGet(get);
        String firstItem = getItems.get(0);
        List<String> toRemove = new ArrayList<>();

        // lookup cache
        for (String item : getItems) {
            CacheEntry<Result> entry = cache.get(item);
            if (entry != null) {
                Result partialResult = entry.getResult();
                byte[] partialResultFamily = partialResult.getMap().keySet().iterator().next();

                result.getMap().putAll(partialResult.getMap());
                toRemove.add(item);
            }

        }

        // CacheEntry<Result> entry = cache.get(Bytes.toInt(get.getRow()) + "::"
        // + key);
        // countGets++;

        if (getItems.size() > 0) {
            Map<String, Get> gets = new HashMap<String, Get>();
            for (String item : getItems) {
                Get g = new Get(get.getRow());

                String[] subItems = item.split(SequenceEngine.SEPARATOR);
                g.addColumn(Bytes.toBytes(subItems[1]), Bytes.toBytes(subItems.length > 2 ? subItems[2] : ""));

                gets.put(tableName, g);
            }

            String key = firstItem;
            Set<String> sequence = sequenceEngine.getSequence(key);
            if (sequence == null) {
                System.out.println("### There is no sequence indexed by key '" + key + "'.");
                result = htable.get(get);
            } else {
                System.out.println("### There are sequences indexed by key '" + key + "'.");

                // FIXME return elements of sequence already batched ?
                // batch updates to the same tables
                Map<String, Get> gets = new HashMap<String, Get>();
                for (String container : sequence) {

                    String[] elements = container.split(":");
                    String containerTableName = elements[0];
                    String containerColFamily = elements[1];
                    String containerColQualifier = elements.length == 3 ? elements[2] : "";

                    Get g = gets.get(containerTableName);
                    if (g == null) {
                        g = new Get(get.getRow());
                    }
                    g.addColumn(Bytes.toBytes(containerColFamily), Bytes.toBytes(containerColQualifier));
                    gets.put(containerTableName, g);

                    StringBuilder sb = new StringBuilder();

                    for (byte b : g.getRow())
                        sb.append(String.format("\\x%02x", b & 0xFF));

                    System.out.println("### ITEM FROM SEQUENCE: tableName: " + containerTableName + ", key: " + sb.toString()
                            + ", colFamily: " + containerColFamily + ", colQualifier: " + containerColQualifier);
                }

                System.out.println("### After batching updates to the same tables: size: " + gets.size());

                // pre-fetch elements to cache
                for (String k : gets.keySet()) {

                    Result r = htables.get(k).get(gets.get(k));
                    if (r.isEmpty())
                        continue;

                    System.out.println("### TableName: " + k + ", Result size: " + r.size());

                    String cacheKey = k;
                    for (byte[] kk : r.getNoVersionMap().keySet()) {
                        System.out.println("### Key kk: " + Bytes.toString(kk));
                        cacheKey += ":" + Bytes.toString(kk);

                        for (byte[] kkk : r.getNoVersionMap().get(kk).keySet()) {
                            System.out.println("### Key kkk: " + Bytes.toString(kkk) + ", Value: "
                                    + Bytes.toString(r.getNoVersionMap().get(kk).get(kkk)));
                            cacheKey += ":" + Bytes.toString(kkk);
                        }
                    }

                    for (KeyValue kv : r.list()) {
                        System.out.println("### Key: " + Bytes.toInt(kv.getKey()) + ", Value: " + Bytes.toString(kv.getValue()));
                    }

                    if (r.size() == 1)
                        cache.put(Bytes.toInt(r.getRow()) + "::" + cacheKey, new CacheEntry<Result>(r));

                }
                System.out.println("### Cache contents: " + cache);
            }
            result = htable.get(get);
        }

        // System.out.println("### Cache hit rate: " + countCacheHits /
        // countGets);

        return result;
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
        // TODO Auto-generated method stub
        return null;
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getTableName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getWriteBufferSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Result increment(Increment arg0) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long incrementColumnValue(byte[] arg0, byte[] arg1, byte[] arg2, long arg3) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long incrementColumnValue(byte[] arg0, byte[] arg1, byte[] arg2, long arg3, boolean arg4) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isAutoFlush() {
        return htable.isAutoFlush();
    }

    @Override
    public RowLock lockRow(byte[] arg0) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void mutateRow(RowMutations arg0) throws IOException {
        // TODO Auto-generated method stub

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
        // TODO Auto-generated method stub

    }

}
