import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;

public class CreateTables {

    private final Connection connection;

    public CreateTables(Connection connection) {
        this.connection = connection;
    }

    public void createAllTables() throws IOException {
            Admin admin = connection.getAdmin();

            createMeasurementsTable(admin);
            createItemsTable(admin);
            createStationsTable(admin);

            admin.close();
    }

    private static void createMeasurementsTable(Admin admin) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("measurements"));

        tableDescriptor.addFamily(new HColumnDescriptor("measurement"));
        tableDescriptor.addFamily(new HColumnDescriptor("instrument"));

        byte[][] splits = {
                Bytes.toBytes("2017-06-01 00:00"),
                Bytes.toBytes("2018-01-01 00:00"),
                Bytes.toBytes("2018-06-01 00:00"),
                Bytes.toBytes("2019-01-01 00:00"),
                Bytes.toBytes("2019-06-01 00:00")
        };

        admin.createTable(tableDescriptor, splits);
        System.out.println("created 'measurements' table");
    }

    private static void createItemsTable(Admin admin) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("items"));

        tableDescriptor.addFamily(new HColumnDescriptor("info"));

        admin.createTable(tableDescriptor);
        System.out.println("created 'items' table");
    }

    private static void createStationsTable(Admin admin) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("stations"));

        tableDescriptor.addFamily(new HColumnDescriptor("info"));

        admin.createTable(tableDescriptor);
        System.out.println("created 'stations' table");
    }
}
