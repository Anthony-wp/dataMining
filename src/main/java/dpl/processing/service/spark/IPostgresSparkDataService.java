package dpl.processing.service.spark;

import dpl.processing.job.context.JobContext;
import dpl.processing.type.Session;
import dpl.processing.vo.wrapper.session.SparkSessionWrapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.time.LocalDateTime;

public interface IPostgresSparkDataService extends ISparkService<SparkSessionWrapper> {
    Dataset<Row> loadBaseTable(String sparkSession, String table);

    Dataset<Row> selectExpr(String sparkSession, String sql);

    Session getInfoSession();

    Dataset<Row> loadBaseTableForOrg(String sparkSession, String table, JobContext context);

    void checkEnableContext();

    void deleteJobTempViews(JobContext tableName);

    String loadBaseTableToViewForOrg(String sparkSession, String table, JobContext context, boolean forceReload);

    Dataset<Row> loadPurchaseDataForOrg(String sparkSession, JobContext context);

    Dataset<Row> loadProductDataForOrg(String sparkSession, JobContext context);

    String loadToViewCustomerExternalIds(String sparkSession, JobContext context);

    String loadToViewEmailIds(String sparkSession, JobContext context);

    String loadToViewOrderIds(String sparkSession, JobContext context);

    String loadToViewProductIds(String sparkSession, JobContext context);
}
