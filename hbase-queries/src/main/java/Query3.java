import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;


/**
 * Q3. Да се прикажат просечната измерена вредност за сите супстанци и исправноста на инструментот, направени на 5. март 2018 од 0:00ч. до 1:00ч. во станицата со код 101.
 */
public class Query3 extends Query {

    public Query3(Connection connection) {
        super(connection);
    }

    @Override
    public String execute() throws Throwable {

        Scan substanceNamesScan = new Scan();
        substanceNamesScan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

        Map<byte[], String> substanceIdToName = new HashMap<>();

        ResultScanner substanceNamesScanner = connection.getTable(TableName.valueOf("items")).getScanner(substanceNamesScan);

        substanceNamesScanner.forEach(result -> substanceIdToName.put(result.getRow(), Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))));

        Get get = new Get(Bytes.toBytes("2018-03-05 00:00+101"));

        substanceIdToName.keySet().forEach(substanceId -> {
            get.addColumn(Bytes.toBytes("measurement"), substanceId);
            get.addColumn(Bytes.toBytes("instrument"), Bytes.toBytes(String.format("status_%s", Bytes.toString(substanceId))));
        });

        Result result = connection.getTable(TableName.valueOf("measurements")).get(get);

        StringBuilder sb = new StringBuilder();

        substanceIdToName.forEach((key, value) -> sb.append(String.format("%s = %f, instrument status = %s\n",
                value,
                Bytes.toDouble(result.getValue(Bytes.toBytes("measurement"), key)),
                Bytes.toString(result.getValue(Bytes.toBytes("instrument"), Bytes.toBytes(String.format("status_%s", Bytes.toString(key)))))
        )));

        return sb.toString();
    }
}
