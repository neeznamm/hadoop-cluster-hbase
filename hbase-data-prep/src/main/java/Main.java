import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();

        config.setInt("timeout", 120000);
        config.set("hbase.zookeeper.quorum","hadoop-master,hadoop-slave1,hadoop-slave2");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("zookeeper.znode.parent", "/hbase");

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            CreateTables createTables = new CreateTables(connection);
            createTables.createAllTables();

            PopulateStationsTable populateStationsTable = new PopulateStationsTable(connection);
            populateStationsTable.execute();

            PopulateItemsTable populateItemsTable = new PopulateItemsTable(connection);
            populateItemsTable.execute();

            PopulateMeasurementsTable populateMeasurementsTable = new PopulateMeasurementsTable(connection);
            populateMeasurementsTable.execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
