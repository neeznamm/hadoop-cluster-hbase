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
 * Q6. Да се прикаже збирот на измерени концентрации на O3 во станицата со код 101 за месец јануари во 2017.
 */
public class Query6 extends Query {

    public Query6(Connection connection)  {
        super(connection);
    }

    @Override
    public String execute() throws Throwable {
        byte[] o3IdBytes = findRowKeyByColumnValue("items","info","name","O3", CompareOperator.EQUAL);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("measurement"), o3IdBytes);
        scan.setFilter(new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("^2017-01-\\d{2} \\d{2}:\\d{2}\\+101$")));

        AggregationClient aggregationClient = new AggregationClient(connection.getConfiguration());
        ColumnInterpreter<Double, Double, HBaseProtos.EmptyMsg, HBaseProtos.DoubleMsg, HBaseProtos.DoubleMsg> columnInterpreter = new DoubleColumnInterpreter();
        double sum = aggregationClient.sum(TableName.valueOf("measurements"), columnInterpreter, scan);

        aggregationClient.close();

        return String.valueOf(sum);
    }

}
