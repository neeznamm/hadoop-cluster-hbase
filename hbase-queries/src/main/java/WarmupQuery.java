import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class WarmupQuery extends Query {

    public WarmupQuery(Connection connection) {
        super(connection);
    }

    @Override
    public String execute() throws Throwable {

        Get get = new Get(Bytes.toBytes("101"));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"));

        Result result = connection.getTable(TableName.valueOf("stations")).get(get);

        return Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("address")));
    }
}
