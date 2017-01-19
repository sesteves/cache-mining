package pt.inescid.gsd.cachemining;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * Created by sesteves on 18-01-2017.
 */
public class DataContainer {

    public final static String SEPARATOR = ":";

    private String tableStr;
    private String rowStr;
    private String familyStr;
    private String qualifierStr;

    private String stringRepresentation;

    private byte[] table;
    private byte[] row;
    private byte[] family;
    private byte[] qualifier;

    public DataContainer(String table, String row, String family, String qualifier) {
        this.tableStr = table;
        this.rowStr = row;
        this.familyStr = family;
        this.qualifierStr = qualifier;

        this.table = Bytes.toBytes(table);
        this.row = Bytes.toBytes(row);
        this.family = Bytes.toBytes(family);
        this.qualifier = Bytes.toBytes(qualifier);

        buildStringRepresentation();
    }

    public DataContainer(byte[] table, byte[] row, byte[] family, byte[] qualifier) {
        this.table = table;
        this.row = row;
        this.family = family;
        this.qualifier = qualifier;

        this.tableStr = Bytes.toString(table);
        this.rowStr = Bytes.toString(row);
        this.familyStr = Bytes.toString(family);
        this.qualifierStr = Bytes.toString(qualifier);

        buildStringRepresentation();
    }

    private void buildStringRepresentation() {
        StringBuilder sb = new StringBuilder(tableStr);
        if (rowStr != null) sb.append(SEPARATOR + rowStr);
        if (familyStr != null) sb.append(SEPARATOR + familyStr);
        if (qualifierStr != null) sb.append(SEPARATOR + qualifierStr);

        stringRepresentation = sb.toString();
    }

    // TODO change deprecated methods
    public static String getKey(String tableName, Cell cell) {
        StringBuilder sb = new StringBuilder(tableName + SEPARATOR + Bytes.toString(cell.getRow()) + SEPARATOR +
                Bytes.toString(cell.getFamily()));

        // TODO check correctness
        if (cell.getQualifierArray() != null) {
            sb.append(SEPARATOR + Bytes.toString(cell.getQualifier()));
        }

        return sb.toString();
    }

    public static String getKey(String tableName, byte[] row, byte[] family) {
        return tableName + SEPARATOR + Bytes.toString(row) + SEPARATOR + Bytes.toString(family);
    }

    public static String getKey(String tableName, byte[] row, byte[] family, byte[] qualifier) {
        return tableName + SEPARATOR + Bytes.toString(row) + SEPARATOR + Bytes.toString(family) + SEPARATOR +
                Bytes.toString(qualifier);
    }

    public String getTableStr() {
        return tableStr;
    }

    public byte[] getTable() {
        return table;
    }

    public byte[] getRow() {
        return row;
    }

    public byte[] getFamily() {
        return family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }


    @Override
    public String toString() {
        return stringRepresentation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataContainer that = (DataContainer) o;

        if (!Arrays.equals(table, that.table)) return false;
        if (!Arrays.equals(row, that.row)) return false;
        if (!Arrays.equals(family, that.family)) return false;
        return Arrays.equals(qualifier, that.qualifier);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(table);
        result = 31 * result + Arrays.hashCode(row);
        result = 31 * result + Arrays.hashCode(family);
        result = 31 * result + Arrays.hashCode(qualifier);
        return result;
    }
}
