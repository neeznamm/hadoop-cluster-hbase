import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;

public class QueryExecutor {
    private static final int NUM_EXECUTIONS = 3;

    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();

        config.setInt("timeout", 120000);
        config.set("hbase.zookeeper.quorum", "hadoop-master,hadoop-slave1,hadoop-slave2");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("zookeeper.znode.parent", "/hbase");

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            List<String> queries = Arrays.asList("Query1", "Query2", "Query3", "Query4", "Query5", "Query6", "Query7");

            for (String queryName : queries) {
                System.out.println("---------------- " + queryName + ":\n");

                Class<?> queryClass = Class.forName(queryName);
                Constructor<?> constructor = queryClass.getConstructor(Connection.class);
                Query queryInstance = (Query) constructor.newInstance(connection);

                long totalExecutionTime = 0;
                String result = null;

                for (int i = 1; i <= NUM_EXECUTIONS; ++i) {
                    long startTime = System.nanoTime();
                    result = queryInstance.execute();
                    long endTime = System.nanoTime();

                    long durationMs = (endTime - startTime) / 1_000_000;
                    totalExecutionTime += durationMs;
                }

                long avgExecutionTime = totalExecutionTime / NUM_EXECUTIONS;
                System.out.println(result);
                System.out.println(avgExecutionTime + " ms\n");
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
