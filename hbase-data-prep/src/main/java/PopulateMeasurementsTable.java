import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class PopulateMeasurementsTable {
    private final Connection connection;

    public PopulateMeasurementsTable(Connection connection) {
        this.connection = connection;
    }

    public void execute() throws IOException {
        Table table = connection.getTable(TableName.valueOf("measurements"));

        try (BufferedReader reader = new BufferedReader(new FileReader("/dataset/Measurement_info.csv"))) {
            reader.readLine(); // skip csv header
            String line;

            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                String measurementDate = columns[0];
                String stationCode = columns[1];
                String itemCode = columns[2];
                String averageValue = columns[3];
                String instrumentStatus = columns[4];

                String rowKey = measurementDate + "+" + stationCode;

                Put putMeasurement = new Put(Bytes.toBytes(rowKey));
                putMeasurement.addColumn(Bytes.toBytes("measurement"), Bytes.toBytes(itemCode), Bytes.toBytes(Double.parseDouble(averageValue)));

                table.put(putMeasurement);
                System.out.println(putMeasurement);

                Put putInstrument = new Put(Bytes.toBytes(rowKey));
                putInstrument.addColumn(Bytes.toBytes("instrument"), Bytes.toBytes(String.format("status_%s", itemCode)), Bytes.toBytes(instrumentStatus));

                table.put(putInstrument);
                System.out.println(putInstrument);
            }
        }
    }

}
