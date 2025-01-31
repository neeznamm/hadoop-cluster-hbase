import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Q4. Да се најде просечната измерена вредност на PM10 кај сите станици за 5. декември 2018.
*/
public class Query4 extends Query {

    public Query4(Connection connection) {
        super(connection);
    }

    @Override
    public String execute() throws Throwable {
        byte[] pm10IdBytes = findRowKeyByColumnValue("items","info","name","PM10", CompareOperator.EQUAL);

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes("2018-12-05")));
        scan.addColumn(Bytes.toBytes("measurement"), pm10IdBytes);

        /*
        * aggregationClient.avg() не дозволува да има повеќе од една фамилија колони во Scan, значи
        * во случајов не може instrument:status колоните да се земат како филтер за редици каде
        * инструментот е исправен. Заради ова се користи следниот филтер, кој отфрла некои
        * погрешни мерења, но не гаранитра дека инструментот бил исправен (вредностите на мерењата
        * кога инструментот не е исправен се најчесто -1.0, но не секогаш)
        */
        scan.setFilter(new SingleColumnValueFilter(
                Bytes.toBytes("measurement"),
                pm10IdBytes,
                CompareOperator.GREATER_OR_EQUAL,
                Bytes.toBytes(0)
        ));

        AggregationClient aggregationClient = new AggregationClient(connection.getConfiguration());
        ColumnInterpreter<Double, Double, HBaseProtos.EmptyMsg, HBaseProtos.DoubleMsg, HBaseProtos.DoubleMsg> columnInterpreter = new DoubleColumnInterpreter();
        double avg = aggregationClient.avg(TableName.valueOf("measurements"), columnInterpreter, scan);

        aggregationClient.close();

        return String.valueOf(avg);

    }
}
