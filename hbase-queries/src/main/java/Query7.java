import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Q7. Да се најдат вредноста, датумот, времето и станицата каде е измерена најголема концентрација на PM2.5 помеѓу 1.8.2019 и 10.8.2019
 */
public class Query7 extends Query {

    public Query7(Connection connection) {
        super(connection);
    }

    @Override
    public String execute() throws Throwable {
        byte[] pm25IdBytes = findRowKeyByColumnValue("items", "info", "name", "PM2.5", CompareOperator.EQUAL);

        Scan maxScan = new Scan();
        maxScan.addColumn(Bytes.toBytes("measurement"), pm25IdBytes);
        maxScan.setFilter(new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("^2019-08-(0[1-9]|10)")));

        AggregationClient aggregationClient = new AggregationClient(connection.getConfiguration());
        ColumnInterpreter<Double, Double, HBaseProtos.EmptyMsg, HBaseProtos.DoubleMsg, HBaseProtos.DoubleMsg> columnInterpreter = new DoubleColumnInterpreter();
        double max = aggregationClient.max(TableName.valueOf("measurements"), columnInterpreter, maxScan);

        aggregationClient.close();

        Scan resultRowScan = new Scan();
        resultRowScan.addColumn(Bytes.toBytes("measurement"), pm25IdBytes);
        resultRowScan.setFilter(new SingleColumnValueFilter(
                Bytes.toBytes("measurement"),
                pm25IdBytes,
                CompareOperator.EQUAL,
                Bytes.toBytes(max)
        ));

        ResultScanner scanner = connection.getTable(TableName.valueOf("measurements")).getScanner(resultRowScan);

        String measurementRowKey = Bytes.toString(scanner.next().getRow());

        scanner.close();

        Get getStation = new Get(Bytes.toBytes(measurementRowKey.split("\\+")[1]));
        getStation.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        Result result = connection.getTable(TableName.valueOf("stations")).get(getStation);
        String stationName = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));

        return String.format("Datum i vreme: %s, Stanica: %s", measurementRowKey.split("\\+")[0], stationName);
    }

}
