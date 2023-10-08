package dpl.processing.vo.wrapper.session;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;

@Getter
@AllArgsConstructor
public class SparkSessionWrapper implements Closeable {

    private static volatile SparkSessionWrapper SESSION;

    private final SparkSession sparkSession;

    @Override
    public void close() {
        sparkSession.close();
    }

    private final Set<String> loadedTables = new HashSet<>();


    public boolean isTableLoaded(String table) {
        return loadedTables.contains(table);
    }

    public void markTableLoaded(String table) {
        loadedTables.add(table);
    }

    public void deleteTable(String table) {
        loadedTables.remove(table);
    }

    public static SparkSessionWrapper getInstance(SparkSession sparkSession) {
        if (SESSION == null) {
            synchronized (SparkSessionWrapper.class) {
                if (SESSION == null) {
                    SESSION = new SparkSessionWrapper(sparkSession);
                }
            }
        }

        return SESSION;
    }
}
