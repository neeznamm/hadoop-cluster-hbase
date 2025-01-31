import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class PopulateItemsTable {
    private final Connection connection;

    public PopulateItemsTable(Connection connection) {
        this.connection = connection;
    }

    public void execute() throws IOException {
        Table table = connection.getTable(TableName.valueOf("items"));

        try (BufferedReader reader = new BufferedReader(new FileReader("/dataset/Measurement_item_info.csv"))) {
            reader.readLine(); // skip csv header
            String line;

            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                String code = columns[0];
                String name = columns[1];
                String unit = columns[2];
                String good = columns[3];
                String normal = columns[4];
                String bad = columns[5];
                String verybad = columns[6];

                Put putItem = new Put(Bytes.toBytes(code));
                putItem.addColumn(Bytes.toBytes("info"), Bytes.toBytes("code"), Bytes.toBytes(code));
                putItem.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
                putItem.addColumn(Bytes.toBytes("info"), Bytes.toBytes("unit"), Bytes.toBytes(unit));
                putItem.addColumn(Bytes.toBytes("info"), Bytes.toBytes("good"), Bytes.toBytes(Double.parseDouble(good)));
                putItem.addColumn(Bytes.toBytes("info"), Bytes.toBytes("normal"), Bytes.toBytes(Double.parseDouble(normal)));
                putItem.addColumn(Bytes.toBytes("info"), Bytes.toBytes("bad"), Bytes.toBytes(Double.parseDouble(bad)));
                putItem.addColumn(Bytes.toBytes("info"), Bytes.toBytes("verybad"), Bytes.toBytes(Double.parseDouble(verybad)));

                table.put(putItem);
                System.out.println(putItem);
            }
        }
    }

}
