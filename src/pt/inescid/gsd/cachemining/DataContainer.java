package pt.inescid.gsd.cachemining;

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
        StringBuilder sb = new StringBuilder();
        sb.append(tableStr);
        if (rowStr != null)
            sb.append(SEPARATOR + rowStr);
        if(familyStr != null)
            sb.append(SEPARATOR + familyStr);
        if(qualifierStr != null)
            sb.append(SEPARATOR + qualifierStr);

        stringRepresentation = sb.toString();
    }

    public byte[] getTable() {
        return table;
    }

    public void setTable(byte[] table) {
        this.table = table;
    }

    public byte[] getRow() {
        return row;
    }

    public void setRow(byte[] row) {
        this.row = row;
    }

    public byte[] getFamily() {
        return family;
    }

    public void setFamily(byte[] family) {
        this.family = family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public void setQualifier(byte[] qualifier) {
        this.qualifier = qualifier;
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
