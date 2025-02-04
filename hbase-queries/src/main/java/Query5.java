import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Q5. Да се најде колку пати во 2018 е измерена вредност на CO помеѓу 2 и 9 (кај сите станици вкупно).
 */
public class Query5 extends Query {

    public Query5(Connection connection)  {
        super(connection);
    }

    @Override
    public String execute() throws Throwable {
        byte[] coIdBytes = findRowKeyByColumnValue("items","info","name","CO", CompareOperator.EQUAL);
        String coId = Bytes.toString(coIdBytes);

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes("2018")));
        scan.addColumn(Bytes.toBytes("measurement"), coIdBytes);
        scan.addColumn(Bytes.toBytes("instrument"), Bytes.toBytes(String.format("status_%s", coId)));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        filterList.addFilter(new PrefixFilter(Bytes.toBytes("2018")));

        // Мерења само кога инструментот е исправен
        filterList.addFilter(new SingleColumnValueFilter(
                Bytes.toBytes("instrument"),
                Bytes.toBytes(String.format("status_%s", coId)),
                CompareOperator.EQUAL,
                Bytes.toBytes("0")
        ));

        filterList.addFilter(new SingleColumnValueFilter(
                Bytes.toBytes("measurement"),
                coIdBytes,
                CompareOperator.GREATER_OR_EQUAL,
                Bytes.toBytes(2.0)
        ));
        filterList.addFilter(new SingleColumnValueFilter(
                Bytes.toBytes("measurement"),
                coIdBytes,
                CompareOperator.LESS_OR_EQUAL,
                Bytes.toBytes(9.0)
        ));
        scan.setFilter(filterList);

        AggregationClient aggregationClient = new AggregationClient(connection.getConfiguration());
        ColumnInterpreter<Double, Double, HBaseProtos.EmptyMsg, HBaseProtos.DoubleMsg, HBaseProtos.DoubleMsg> columnInterpreter = new DoubleColumnInterpreter();
        long count = aggregationClient.rowCount(TableName.valueOf("measurements"), columnInterpreter, scan);

        aggregationClient.close();

        return String.valueOf(count);
    }

}
