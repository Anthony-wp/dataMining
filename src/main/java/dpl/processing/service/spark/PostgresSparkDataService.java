package dpl.processing.service.spark;

import dpl.processing.config.SparkConnectionSource;
import dpl.processing.config.SparkSessionConfig;
import dpl.processing.job.context.JobContext;
import dpl.processing.type.Session;
import dpl.processing.utils.StringUtils;
import dpl.processing.vo.wrapper.session.SparkSessionWrapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalog.Table;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static dpl.processing.constants.PostgresConstants.*;
import static dpl.processing.type.PostgresAppTables.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class PostgresSparkDataService implements IPostgresSparkDataService {
    private final SparkSessionConfig sparkSessionConfig;

    @Getter
    private final Map<String, SparkSessionWrapper> sparkSessions = new ConcurrentHashMap<>();

    @Override
    public SparkConnectionSource getSparkConfig() {
        return sparkSessionConfig.getPostgres();
    }

    @Override
    public String getFormat() {
        return "jdbc";
    }

    @Override
    public SparkSessionWrapper buildSessionWrapper(SparkSession sparkSession) {
        return SparkSessionWrapper.getInstance(sparkSession);
    }

    @Override
    public Dataset<Row> loadBaseTable(String sparkSession, String table) {
        return getSparkSession(sparkSession)
                .read()
                .format(getFormat())
                .option("url", "jdbc:postgresql://172.30.0.4:5432/shop_data")
                .option("user", "postgres")
                .option("password", "1qaz2wsXX")
                .option("driver", "org.postgresql.Driver")
                .option(TABLE, String.format("%s.%s", SHOP_GLOBAL_KEYSPACE, table))
                .load();
    }

    @Override
    public Dataset<Row> selectExpr(String sparkSession, String sql) {
        return getSparkSession(sparkSession)
                .sql(sql);
    }

    @Override
    public Session getInfoSession() {
        return Session.DPL_CASS_PROCESSING;
    }

    @Override
    public Dataset<Row> loadBaseTableForOrg(String sparkSession, String table, JobContext context) {
        return loadBaseTable(sparkSession, table);
    }

    @Override
    public void deleteJobTempViews(JobContext context) {

        Catalog catalog = getSparkSession(getInfoSession().getName())
                .catalog();

        String tableName = buildTableName(context, "");

        List<String> deleteTables = catalog
                .listTables()
                .collectAsList()
                .stream()
                .map(Table::name)
                .filter(name -> name.contains(tableName))
                .collect(Collectors.toList());

        deleteTables.forEach(catalog::dropTempView);
        deleteTables.forEach(sparkSessions.get(getInfoSession().getName())::deleteTable);
    }

    @Override
    public Dataset<Row> loadProductDataForOrg(String sparkSession, JobContext context) {
        return loadBaseTableForOrg(sparkSession, PRODUCT_TABLE.getTableName(), context);
    }

    @Override
    public String loadBaseTableToViewForOrg(String sparkSession, String table, JobContext context, boolean forceReload) {

        SparkSessionWrapper sparkSessionWrapper = getSparkSessionWrapper(sparkSession);

        String tempTableName = buildTableName(context, table);

        if (!sparkSessionWrapper.isTableLoaded(table) || forceReload) {
            loadBaseTableForOrg(sparkSession, table, context).createOrReplaceTempView(tempTableName);
            sparkSessionWrapper.markTableLoaded(tempTableName);
        }

        return table;
    }

    private String loadBaseTableToViewForOrg(String sparkSession, String table, JobContext context) {
        return loadBaseTableToViewForOrg(sparkSession, table, context, false);
    }

    @Override
    public Dataset<Row> loadPurchaseDataForOrg(String sparkSession, JobContext context) {
        log.info("Count of the spark session {}", getSparkSessions().size());

        Dataset<Row> orderData = loadBaseTableForOrg(sparkSession, "order", context);
        Dataset<Row> userData = table(sparkSession, buildTableName(context, loadUserData(sparkSession, context)))
                .select(USER_ID_FIELD);

        return orderData.join(userData, orderData.col(ORDER_CUSTOMER_ID_FIELD)
                .equalTo(userData.col(USER_ID_FIELD)));
    }

    @Override
    public String loadUserData(String sparkSession, JobContext context) {
        return loadBaseTableToViewForOrg(sparkSession, USER_TABLE.getTableName(), context);
    }

    @Override
    public void checkEnableContext() {
        String sessionName = getInfoSession().getName();
        if(getSparkSession(sessionName) == null || getSparkSession(sessionName).sparkContext() == null) {
            log.error("Spark context is closed, {}", new Date());
        }
    }

    @Override
    public String loadOrderData(String sparkSession, JobContext context) {
        return loadBaseTableToViewForOrg(sparkSession, ORDER_TABLE.getTableName(), context);
    }

    private String buildTableName(JobContext context, String table) {
        return StringUtils.concatToColumn(table);
    }
}
