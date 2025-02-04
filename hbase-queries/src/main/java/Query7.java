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
 * Q7. Да се наjде наjголемата концентрациjа на PM2.5 измерена помеѓу 1.8.2019 и 10.8.2019.
 */
public class Query7 extends Query {

    public Query7(Connection connection) {
        super(connection);
    }

    @Override
    public String execute() throws Throwable {
        byte[] pm25IdBytes = findRowKeyByColumnValue("items", "info", "name", "PM2.5", CompareOperator.EQUAL);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("measurement"), pm25IdBytes);
        scan.setFilter(new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("^2019-08-(0[1-9]|10)")));

        AggregationClient aggregationClient = new AggregationClient(connection.getConfiguration());
        ColumnInterpreter<Double, Double, HBaseProtos.EmptyMsg, HBaseProtos.DoubleMsg, HBaseProtos.DoubleMsg> columnInterpreter = new DoubleColumnInterpreter();
        double max = aggregationClient.max(TableName.valueOf("measurements"), columnInterpreter, scan);

        aggregationClient.close();

        return String.valueOf(max);
    }

}
