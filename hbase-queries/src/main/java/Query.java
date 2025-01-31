import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public abstract class Query {
    Connection connection;

    public Query(Connection connection) {
        this.connection = connection;
    }

    abstract String execute() throws Throwable;

    byte[] findRowKeyByColumnValue(String tableName, String columnPrefix, String columnQualifier, String compareValue, CompareOperator compareOperator)
            throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        byte[] columnPrefixBytes = Bytes.toBytes(columnPrefix);
        byte[] columnQualifierBytes = Bytes.toBytes(columnQualifier);
        byte[] compareValueBytes = Bytes.toBytes(compareValue);

        Scan scan = new Scan();
        scan.addColumn(columnPrefixBytes, columnQualifierBytes);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        filterList.addFilter(new SingleColumnValueFilter(
                columnPrefixBytes,
                columnQualifierBytes,
                compareOperator,
                compareValueBytes
        ));
        filterList.addFilter(new KeyOnlyFilter());

        scan.setFilter(filterList);

        ResultScanner scanner = table.getScanner(scan);

        byte[] rowKeyBytes = scanner.next().getRow();

        scanner.close();

        table.close();

        return rowKeyBytes;
    }
}
