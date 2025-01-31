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
        try (Admin admin = connection.getAdmin()) {
            createMeasurementsTable(admin);
            createItemsTable(admin);
            createStationsTable(admin);
        }
    }

    private static void createMeasurementsTable(Admin admin) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("measurements"));
        tableDescriptor.addFamily(new HColumnDescriptor("measurement"));
        tableDescriptor.addFamily(new HColumnDescriptor("instrument"));

        byte[][] splits = {Bytes.toBytes("2018-06-01 00:00")};

        admin.createTable(tableDescriptor, splits);
        System.out.println("Created 'measurements' table with one split at 2018-06-01 00:00");
    }

    private static void createItemsTable(Admin admin) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("items"));
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("info");

        columnDescriptor.setInMemory(true);
        tableDescriptor.addFamily(columnDescriptor);

        admin.createTable(tableDescriptor);
        System.out.println("Created 'items' table (stored in memory)");
    }

    private static void createStationsTable(Admin admin) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("stations"));
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("info");

        columnDescriptor.setInMemory(true);
        tableDescriptor.addFamily(columnDescriptor);

        admin.createTable(tableDescriptor);
        System.out.println("Created 'stations' table (stored in memory)");
    }
}
