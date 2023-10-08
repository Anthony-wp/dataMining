package dpl.processing.service.spark;

import dpl.processing.config.SparkConnectionSource;
import dpl.processing.job.context.JobContext;
import dpl.processing.vo.wrapper.session.SparkSessionWrapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.annotation.PreDestroy;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public interface ISparkService<S extends SparkSessionWrapper> {
    Map<String, S> getSparkSessions();

    SparkConnectionSource getSparkConfig();

    String getFormat();

    default void stopSparkSession(String sparkJobName) {
        Optional.ofNullable(getSparkSessions().remove(sparkJobName)).map(SparkSessionWrapper::getSparkSession).ifPresent(SparkSession::close);
    }

    @PreDestroy
    default void stopAll() {
        getSparkSessions().forEach((name, session) -> session.close());
        getSparkSessions().clear();
    }

    default S getSparkSessionWrapper(String sparkJobName) {
        return Optional.ofNullable(getSparkSessions().get(sparkJobName))
                .orElseGet(() -> startNewSparkSession(sparkJobName));
    }

    default SparkSession getSparkSession(String sparkJobName) {
        return getSparkSessionWrapper(sparkJobName).getSparkSession();
    }

    default Dataset<Row> table(String sparkJobName, String table) {
        SparkSession sparkSession = getSparkSession(sparkJobName);

        if (!sparkSession.catalog().tableExists(table)) {
            throw new IllegalArgumentException("No table " + table + " found in spark session " + sparkJobName);
        }

        return sparkSession.table(table);
    }

    default <T> Dataset<T> createOneColumnDataset(String sparkJobName, List<T> list, Encoder<T> encoder) {
        SparkSession sparkSession = getSparkSession(sparkJobName);

        return sparkSession.createDataset(list, encoder);
    }

    default S startNewSparkSession(String sparkJobName) {

        SparkSession.Builder sparkSessionBuilder = SparkSession.builder()
                .appName(sparkJobName)
                .config(getSparkConfig().getConfigs());

        S sparkSession = buildSessionWrapper(sparkSessionBuilder.getOrCreate());
        getSparkSessions().put(sparkJobName, sparkSession);
        return sparkSession;
    }

    S buildSessionWrapper(SparkSession sparkSession);
}
