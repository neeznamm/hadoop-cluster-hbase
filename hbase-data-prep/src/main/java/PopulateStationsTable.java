import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class PopulateStationsTable {
    private final Connection connection;

    public PopulateStationsTable(Connection connection) {
        this.connection = connection;
    }

    public void execute() throws IOException {
        Table table = connection.getTable(TableName.valueOf("stations"));

        try (BufferedReader reader = new BufferedReader(new FileReader("/dataset/Measurement_station_info.csv"))) {
            reader.readLine(); // skip csv header
            String line;

            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                String code = columns[0];
                String name = columns[1];
                String address = columns[2];
                String latitude = columns[3];
                String longitude = columns[4];

                Put putStation = new Put(Bytes.toBytes(code));
                putStation.addColumn(Bytes.toBytes("info"), Bytes.toBytes("code"), Bytes.toBytes(code));
                putStation.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
                putStation.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes(address));
                putStation.addColumn(Bytes.toBytes("info"), Bytes.toBytes("latitude"), Bytes.toBytes(Double.parseDouble(latitude)));
                putStation.addColumn(Bytes.toBytes("info"), Bytes.toBytes("longitude"), Bytes.toBytes(Double.parseDouble(longitude)));

                table.put(putStation);
                System.out.println(putStation);
            }
        }
    }

}
