package pt.inescid.gsd.cachemining.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class OperationRegionObserver extends BaseRegionObserver {

    @Override
    public void preGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {

        // e.bypass();

        String rowStr = Bytes.toString(get.getRow());
        String tableName = e.getEnvironment().getRegion().getTableDesc().getNameAsString();
        long ts = System.currentTimeMillis();
        Set<byte[]> families = get.familySet();
        for (byte[] f : families) {
            NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(f);
            if (qualifiers != null) {
                for (byte[] q : qualifiers) {
                    System.out.println(ts + ":" + tableName + ":" + rowStr + ":" + Bytes.toString(f) + ":" + Bytes.toString(q));
                }
            } else {
                System.out.println(ts + ":" + tableName + ":" + rowStr + ":" + Bytes.toString(f));
            }
        }
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL)
            throws IOException {

        super.prePut(e, put, edit, writeToWAL);
    }

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
            throws IOException {

        String tableName = e.getEnvironment().getRegion().getTableDesc().getNameAsString();

        String rowStr = Bytes.toString(scan.getStartRow()) + ":" + Bytes.toString(scan.getStopRow());
        long ts = System.currentTimeMillis();
        Set<byte[]> families = scan.getFamilyMap().keySet();
        for (byte[] f : families) {
            NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(f);
            if (qualifiers != null) {
                for (byte[] q : qualifiers) {
                    System.out.println(ts + ":" + tableName + ":" + rowStr + ":" + Bytes.toString(f) + ":" + Bytes.toString(q));
                }
            } else {
                System.out.println(ts + ":" + tableName + ":" + rowStr + ":" + Bytes.toString(f));
            }
        }

        return super.preScannerOpen(e, scan, s);
    }
}
