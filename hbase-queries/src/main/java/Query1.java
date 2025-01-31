import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Q1. Да се најде адресата, географската ширина и должина на станицата со код 108.
 */
public class Query1 extends Query {

    public Query1(Connection connection) {
        super(connection);
    }

    @Override
    public String execute() throws Throwable {

        Get get = new Get(Bytes.toBytes("108"));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("latitude"));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("longitude"));

        Result result = connection.getTable(TableName.valueOf("stations")).get(get);

        String address = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("address")));
        double latitude = Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("latitude")));
        double longitude = Bytes.toDouble(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("longitude")));

        return String.format("Adresa: %s, Sirina: %f, Dolzina: %f", address, latitude, longitude);
    }
}
