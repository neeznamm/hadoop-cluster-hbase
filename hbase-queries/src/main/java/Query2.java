import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * Q2. Да се прикажат имињата на супстанците чија мерна единица е ppm.
 */
public class Query2 extends Query {

    public Query2(Connection connection) {
        super(connection);
    }

    @Override
    public String execute() throws Throwable {

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("unit"));
        scan.setFilter(new SingleColumnValueFilter(
                Bytes.toBytes("info"),
                Bytes.toBytes("unit"),
                CompareOperator.EQUAL,
                Bytes.toBytes("ppm")
        ));

        ResultScanner scanner = connection.getTable(TableName.valueOf("items")).getScanner(scan);

        List<String> substanceNames = new ArrayList<>();
        scanner.forEach(result -> substanceNames.add(Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("name")))));

        return substanceNames.toString();
    }
}
