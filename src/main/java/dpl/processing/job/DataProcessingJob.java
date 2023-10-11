package dpl.processing.job;

import dpl.processing.model.AggregatedData;
import dpl.processing.job.context.ProcessJobContext;
import dpl.processing.model.CardHeader;
import dpl.processing.service.AggregatedDataService;
import dpl.processing.service.spark.IPostgresSparkDataService;
import dpl.processing.vo.wrapper.row.SimpleRowWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.collection.Seq;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static dpl.processing.constants.PostgresConstants.CUSTOMER_KEY_FIELD;
import static dpl.processing.constants.PostgresConstants.ENGAGEMENT_GAP_SECONDS_FIELD;
import static dpl.processing.utils.DateUtils.asDate;
import static dpl.processing.utils.DateUtils.generateWeekTimeKeys;
import static dpl.processing.utils.ScalaUtils.toSeq;
import static dpl.processing.vo.ColumnsNameConstants.*;
import static org.apache.spark.sql.functions.*;

@Slf4j
@Component
public class DataProcessingJob {
    protected static final String WEEK_DATE_FIELD = "week_date";
    protected static final String DAY_DATE_FIELD = "day_date";

    protected static final String OLDEST_ORDER_TIMESTAMP_FIELD = "oldest_order_timestamp_5y_cut";
    protected static final String OLDEST_ORDER_TIMESTAMP_FIELD_W_CUT = "oldest_order_timestamp";
    protected static final String FIRST_ORDER_ID_FIELD = "first_order_id";
    protected static final String JOB_DATE_FIELD = "job_date";
    protected static final String TOTAL_VALUE_FIELD = "total_value";
    protected static final String CUSTOMER_TOTAL_VALUE_FIELD = "customer_total_value";
    protected static final String TOTAL_VALUE_YEAR_FIELD = "year_total_value";
    protected static final String TOTAL_VALUE_QUARTER_FIELD = "quarter_total_value";
    protected static final String TOTAL_VALUE_60_DAYS_FIELD = "d60_total_value";
    protected static final String TOTAL_VALUE_90_DAYS_FIELD = "d90_total_value";
    protected static final String TOTAL_VALUE_30_DAYS_FIELD = "d30_total_value";
    protected static final String TOTAL_VALUE_WEEK_FIELD = "last_week_total_value";

    protected static final String AVG_BY_DAY_WEEK_FIELD = "avg_by_day_week_value";
    protected static final String YESTERDAY_TOTAL_VALUE_FIELD = "yesterday_total_value";
    protected static final String LAST_WEEK_ORDER_NUMBER_FIELD = "last_week_num_of_orders";
    protected static final String YESTERDAY_ORDER_NUMBER_FIELD = "yesterday_num_of_orders";
    protected static final String LAST_WEEK_ANG_ORDER_VALUE_FIELD = "last_week_avg_order_value";
    protected static final String YESTERDAY_ANG_ORDER_VALUE_FIELD = "yesterday_avg_order_value";

    protected static final String QUARTER2_LOWER_BOUND = "q2_lower_bound";
    protected static final String QUARTER3_LOWER_BOUND = "q3_lower_bound";
    protected static final String QUARTER4_LOWER_BOUND = "q4_lower_bound";

    protected static final String ORDERS_COUNT_QUARTER1 = "orders_count_quarter_1";
    protected static final String ORDERS_COUNT_QUARTER2 = "orders_count_quarter_2";
    protected static final String ORDERS_COUNT_QUARTER3 = "orders_count_quarter_3";
    protected static final String ORDERS_COUNT_QUARTER4 = "orders_count_quarter_4";

    protected static final String ENGAGEMENT_FIRST_TIMESTAMP_FIELD = "engagement_first_timestamp";
    protected static final String ENGAGEMENT_LAST_TIMESTAMP_FIELD = "engagement_last_timestamp";
    protected static final String ENGAGEMENT_EVENTS_COUNT = "engagement_events_count";

    protected static final String LAST_30_DAYS_ACTIVE_CUSTOMERS = "last_30_days_active_customers";
    protected static final String LAST_30_DAYS_LOST_CUSTOMERS = "last_30_days_lost_customers";
    protected static final String RISK_OF_LEAVING_CUSTOMERS = "risk_of_leaving_customers";
    protected static final String LOYAL_RISK_OF_LEAVING_CUSTOMERS = "loyal_risk_of_leaving_customers";

    protected static final String IS_LOYAL_FIELD = "is_loyal";
    public static final String IS_ACTIVE_FIELD = "is_active";
    protected static final String IS_AT_RISK_FIELD = "is_at_risk";

    public static final String ORDER_NUMBER_FIELD = "num_of_orders";
    public static final String AVG_ORDER_VALUE_FIELD = "avg_order_value";
    public static final String ITEMS_NUMBER_FIELD = "num_of_items";
    public static final String DIFF_PRODUCTS_NUMBER_FIELD = "num_of_diff_products";
    public static final String CUSTOMER_NUMBER_FIELD = "num_of_customers";
    public static final String VALUE_FIELD = "value";

    protected static final String LEFT_JOIN = "left";
    protected static final Seq<String> CUSTOMER_KEY_TO_JOIN = toSeq(Collections.singletonList(CUSTOMER_KEY_FIELD));
    public static final List<String> ORDER_PAID_STATUSES = Arrays.asList("PAID", "PARTIALLY_PAID", "PARTIALLY_REFUNDED");


    @Autowired
    protected IPostgresSparkDataService sparkDataService;
    @Autowired
    protected AggregatedDataService dataService;

//    public final void startJob() {
//        String sessionName = sparkDataService.getInfoSession().getName();
//        Dataset<Row> orders = sparkDataService.loadBaseTable(sessionName, "testshop.orders");
//
//        System.out.println(orders.collectAsList());
//
//    }

    public final void startJob(ProcessJobContext context) {
        String sessionName = sparkDataService.getInfoSession().getName();

        LocalDateTime jobDate = context.getJobEntryTimestamp();

        log.trace("{} job: using '{}' spark session", getJobName(), sessionName);

        Dataset<Row> ordersDataSet = sparkDataService.loadPurchaseDataForOrg(sessionName, context)
                .where(col(ORDER_LINE_ITEM_TIMESTAMP).lt(Timestamp.valueOf(jobDate))
                        .and(upper(col(ORDER_PAYMENT_STATUS)).isin(ORDER_PAID_STATUSES.toArray())))
                .withColumn(WEEK_DATE_FIELD, date_trunc("WEEK", col(ORDER_LINE_ITEM_TIMESTAMP)))
                .withColumn(TOTAL_VALUE_FIELD, coalesce(col(LINE_VALUE_INCLUDING_TAX), col(LINE_VALUE_EXCLUDING_TAX), lit(0))
                                .multiply(coalesce(col(ORDER_LINE_ITEM_QTY), lit(1)))

                        //TODO: Re-instate removal of order discount amount when done in spend bracket calcs too
                        //.$minus(coalesce(col(ORDER_DISCOUNT_AMOUNT), lit(0)))
                );

        if (log.isTraceEnabled()) {
            log.trace("{} job: loaded ORDER dataset with {} rows", getJobName(), ordersDataSet.count());
        }

        AggregatedData aggregatedData = startProcessingData(context, sessionName, jobDate, ordersDataSet);

        generateCardData(aggregatedData, jobDate, ordersDataSet);
        dataService.saveData(aggregatedData);

    }

    private AggregatedData startProcessingData(ProcessJobContext context, String sessionName, LocalDateTime jobDate, Dataset<Row> ordersDataSet) {
        return new AggregatedData();
    }

    private void generateCardData(AggregatedData aggregatedData, LocalDateTime currentDay, Dataset<Row> ordersDataSet) {
        Column timestampColumn = col(ORDER_LINE_ITEM_TIMESTAMP);

        log.trace("{} job: generating card header", getJobName());

        Dataset<Row> avgByDayOfWeekData = ordersDataSet
                .filter(timestampColumn.$greater$eq(Timestamp.valueOf(currentDay.minusDays(7))))
                .withColumn(DAY_DATE_FIELD, date_trunc("DAY", col(ORDER_LINE_ITEM_TIMESTAMP)))
                .groupBy(DAY_DATE_FIELD)
                .agg(
                        sum(TOTAL_VALUE_FIELD).as(AVG_BY_DAY_WEEK_FIELD)
                )
                .select(avg(col(AVG_BY_DAY_WEEK_FIELD)).as(AVG_BY_DAY_WEEK_FIELD));

        BigDecimal last7daysAverageSales = BigDecimal.valueOf(new SimpleRowWrapper(avgByDayOfWeekData
                .collectAsList().get(0)).getDouble(AVG_BY_DAY_WEEK_FIELD, 0D));
        SimpleRowWrapper headerData = new SimpleRowWrapper(ordersDataSet.agg(

                        sum(col(TOTAL_VALUE_FIELD)).as(TOTAL_VALUE_FIELD),
                        sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(currentDay.minusYears(1))), col(TOTAL_VALUE_FIELD)))
                                .as(TOTAL_VALUE_YEAR_FIELD),
                        sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(currentDay.minusDays(7))), col(TOTAL_VALUE_FIELD)))
                                .as(TOTAL_VALUE_WEEK_FIELD),
                        sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(currentDay.minusDays(1))), col(TOTAL_VALUE_FIELD)))
                                .as(YESTERDAY_TOTAL_VALUE_FIELD),

                        countDistinct(when(timestampColumn.$greater$eq(Timestamp.valueOf(currentDay.minusYears(1))), col(ORDER_ID)))
                                .alias(ORDER_NUMBER_FIELD),
                        countDistinct(when(timestampColumn.$greater$eq(Timestamp.valueOf(currentDay.minusDays(7))), col(ORDER_ID)))
                                .alias(LAST_WEEK_ORDER_NUMBER_FIELD),
                        countDistinct(when(timestampColumn.$greater$eq(Timestamp.valueOf(currentDay.minusDays(1))), col(ORDER_ID)))
                                .alias(YESTERDAY_ORDER_NUMBER_FIELD)
                ).withColumn(AVG_ORDER_VALUE_FIELD, col(TOTAL_VALUE_FIELD).divide(col(ORDER_NUMBER_FIELD)))
                .withColumn(LAST_WEEK_ANG_ORDER_VALUE_FIELD, col(TOTAL_VALUE_WEEK_FIELD).divide(col(LAST_WEEK_ORDER_NUMBER_FIELD)))
                .withColumn(YESTERDAY_ANG_ORDER_VALUE_FIELD, col(YESTERDAY_TOTAL_VALUE_FIELD).divide(col(YESTERDAY_ORDER_NUMBER_FIELD)))
                .collectAsList().get(0));

        aggregatedData.setTotalSales(toBigDecimal(headerData.getDouble(TOTAL_VALUE_YEAR_FIELD, 0.0)));
        aggregatedData.setYesterdaySales(toBigDecimal(headerData.getDouble(YESTERDAY_TOTAL_VALUE_FIELD, 0.0)));
        aggregatedData.setYesterdayAverageOrderValue(toBigDecimal(headerData.getDouble(YESTERDAY_ANG_ORDER_VALUE_FIELD, 0.0)));
        aggregatedData.setLast7daysAverageSales(last7daysAverageSales);
        aggregatedData.setLast7daysAverageOrderValue(toBigDecimal(headerData.getDouble(LAST_WEEK_ANG_ORDER_VALUE_FIELD, 0.0)));
    }
    private SimpleRowWrapper getCustomerSegmentDataFromCustomerDataSet(LocalDateTime currentDay,
                                                                       Dataset<Row> ordersDataSet) {
        return new SimpleRowWrapper(
                createCustomerStatsDataset(currentDay, ordersDataSet)
                        .agg(
                                countDistinct(when(col(TOTAL_VALUE_30_DAYS_FIELD).$greater(lit(0))
                                        .or(col(ENGAGEMENT_LAST_TIMESTAMP_FIELD).$greater(Timestamp.valueOf(currentDay.minusDays(30)))), col(CUSTOMER_KEY_FIELD)))
                                        .alias(LAST_30_DAYS_ACTIVE_CUSTOMERS),
                                countDistinct(when(col(TOTAL_VALUE_30_DAYS_FIELD).equalTo(lit(0))
                                        .and(col(ENGAGEMENT_LAST_TIMESTAMP_FIELD).isNull().or(col(ENGAGEMENT_LAST_TIMESTAMP_FIELD)
                                                .$less(Timestamp.valueOf(currentDay.minusDays(30))))), col(CUSTOMER_KEY_FIELD)))
                                        .alias(LAST_30_DAYS_LOST_CUSTOMERS),
                                countDistinct(when(col(IS_AT_RISK_FIELD).equalTo(lit(true)), col(CUSTOMER_KEY_FIELD)))
                                        .alias(RISK_OF_LEAVING_CUSTOMERS),
                                countDistinct(when(col(IS_AT_RISK_FIELD).equalTo(lit(true)).and(col(IS_LOYAL_FIELD).equalTo(lit(true))), col(CUSTOMER_KEY_FIELD)))
                                        .alias(LOYAL_RISK_OF_LEAVING_CUSTOMERS)
                        ).collectAsList().get(0));
    }

    protected Dataset<Row> createCustomerStatsDataset(LocalDateTime currentDay, Dataset<Row> ordersDataSet) {
        log.trace("{} job: loading user stats dataset", getJobName());

        Column timestampColumn = col(ORDER_LINE_ITEM_TIMESTAMP);

        Dataset<Row> dates = ordersDataSet.groupBy(CUSTOMER_KEY_FIELD)
                .agg(
                        min(timestampColumn).as(OLDEST_ORDER_TIMESTAMP_FIELD_W_CUT),
                        when(min(timestampColumn).gt(Timestamp.valueOf(currentDay.minusYears(5))), min(timestampColumn))
                                .otherwise(Timestamp.valueOf(currentDay.minusYears(5))).as(OLDEST_ORDER_TIMESTAMP_FIELD),
                        lit(Timestamp.valueOf(currentDay)).as(JOB_DATE_FIELD)
                )
                .withColumn(QUARTER2_LOWER_BOUND, expr("date_sub(job_date, datediff(job_date, oldest_order_timestamp) / 4)").cast("timestamp"))
                .withColumn(QUARTER3_LOWER_BOUND, expr("date_sub(job_date, datediff(job_date, oldest_order_timestamp) / 2)").cast("timestamp"))
                .withColumn(QUARTER4_LOWER_BOUND, expr("date_sub(job_date, datediff(job_date, oldest_order_timestamp) * 3 / 4)").cast("timestamp"));

        if (log.isTraceEnabled()) {
            log.trace("{} job: loaded CUSTOMER data with {} rows", getJobName(), dates.count());
        }


        log.trace("{} job: Constructed ENGAGEMENT dataset", getJobName());
        WindowSpec window = Window.partitionBy(CUSTOMER_KEY_FIELD).orderBy(col(ORDER_LINE_ITEM_TIMESTAMP).asc_nulls_last());

        return ordersDataSet
                .withColumn(FIRST_ORDER_ID_FIELD, first(col(ORDER_ID), true).over(window))
                .join(dates, CUSTOMER_KEY_TO_JOIN, LEFT_JOIN)
                .groupBy(col(CUSTOMER_KEY_FIELD))
                .agg(
                        first(FIRST_ORDER_ID_FIELD).alias(FIRST_ORDER_ID_FIELD),
                        countDistinct(col(ORDER_ID)).alias(ORDER_NUMBER_FIELD),
                        coalesce(sum(col(TOTAL_VALUE_FIELD)), lit(0)).as(CUSTOMER_TOTAL_VALUE_FIELD),
                        coalesce(sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(currentDay.minusYears(1))), col(TOTAL_VALUE_FIELD))), lit(0))
                                .as(TOTAL_VALUE_YEAR_FIELD),
                        coalesce(sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(currentDay.minusDays(90))), col(TOTAL_VALUE_FIELD))), lit(0))
                                .as(TOTAL_VALUE_QUARTER_FIELD),
                        coalesce(sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(currentDay.minusDays(90))), col(TOTAL_VALUE_FIELD))), lit(0))
                                .as(TOTAL_VALUE_90_DAYS_FIELD),
                        coalesce(sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(currentDay.minusDays(60))), col(TOTAL_VALUE_FIELD))), lit(0))
                                .as(TOTAL_VALUE_60_DAYS_FIELD),
                        coalesce(sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(currentDay.minusDays(30))), col(TOTAL_VALUE_FIELD))), lit(0))
                                .as(TOTAL_VALUE_30_DAYS_FIELD),
                        countDistinct(when(timestampColumn.lt(col(QUARTER2_LOWER_BOUND)), when(timestampColumn.$greater$eq(col(OLDEST_ORDER_TIMESTAMP_FIELD)), col(ORDER_ID)))).as(ORDERS_COUNT_QUARTER1),
                        countDistinct(when(timestampColumn.lt(col(QUARTER3_LOWER_BOUND)), when(timestampColumn.$greater$eq(col(QUARTER2_LOWER_BOUND)), col(ORDER_ID)))).as(ORDERS_COUNT_QUARTER2),
                        countDistinct(when(timestampColumn.lt(col(QUARTER4_LOWER_BOUND)), when(timestampColumn.$greater$eq(col(QUARTER3_LOWER_BOUND)), col(ORDER_ID)))).as(ORDERS_COUNT_QUARTER3),
                        countDistinct(when(timestampColumn.$less$eq(col(JOB_DATE_FIELD)), when(timestampColumn.$greater$eq(col(QUARTER4_LOWER_BOUND)), col(ORDER_ID)))).as(ORDERS_COUNT_QUARTER4),
                        first(col(OLDEST_ORDER_TIMESTAMP_FIELD_W_CUT), true).as(OLDEST_ORDER_TIMESTAMP_FIELD_W_CUT),
                        first(col(OLDEST_ORDER_TIMESTAMP_FIELD), true).as(OLDEST_ORDER_TIMESTAMP_FIELD),
                        first(col(JOB_DATE_FIELD), true).as(JOB_DATE_FIELD),
                        first(col(ENGAGEMENT_LAST_TIMESTAMP_FIELD), true).as(ENGAGEMENT_LAST_TIMESTAMP_FIELD),
                        first(col(ENGAGEMENT_FIRST_TIMESTAMP_FIELD), true).as(ENGAGEMENT_FIRST_TIMESTAMP_FIELD),
                        first(col(ENGAGEMENT_EVENTS_COUNT), true).as(ENGAGEMENT_EVENTS_COUNT),
                        first(col(ENGAGEMENT_GAP_SECONDS_FIELD), true).as(ENGAGEMENT_GAP_SECONDS_FIELD)
                )
                .withColumn(IS_LOYAL_FIELD, col(ORDERS_COUNT_QUARTER1).plus(col(ORDERS_COUNT_QUARTER3)).$greater(lit(0))
                        .and(col(ORDERS_COUNT_QUARTER2).plus(col(ORDERS_COUNT_QUARTER4)).$greater(lit(0))))
                .withColumn(IS_ACTIVE_FIELD, col(TOTAL_VALUE_90_DAYS_FIELD).$greater(lit(0))
                        .or(col(ENGAGEMENT_LAST_TIMESTAMP_FIELD).$greater(Timestamp.valueOf(currentDay.minusDays(90)))))
                .withColumn(IS_AT_RISK_FIELD, col(CUSTOMER_TOTAL_VALUE_FIELD).$greater(col(TOTAL_VALUE_YEAR_FIELD))
                        .and(col(TOTAL_VALUE_YEAR_FIELD).$greater(col(TOTAL_VALUE_60_DAYS_FIELD)))
                        .and(col(TOTAL_VALUE_60_DAYS_FIELD).equalTo(lit(0)))
                );

    }


    protected BigDecimal toBigDecimal(double value) {
        return round(BigDecimal.valueOf(value));
    }

    protected BigDecimal round(BigDecimal value) {
        return value.setScale(2, RoundingMode.HALF_UP);
    }

    protected Dataset<Row> getWeekData(String sparkSession, LocalDateTime currentDay, Dataset<Row> ordersDataSet) {
        // data for all weeks
        Dataset<Row> weekData = ordersDataSet.groupBy(WEEK_DATE_FIELD)
                .agg(
                        countDistinct(col(ORDER_ID)).alias(ORDER_NUMBER_FIELD),
                        countDistinct(CUSTOMER_KEY_FIELD).alias(CUSTOMER_NUMBER_FIELD),
                        sum(col(ORDER_LINE_ITEM_QTY)).alias(ITEMS_NUMBER_FIELD),
                        sum(TOTAL_VALUE_FIELD).alias(TOTAL_VALUE_FIELD),
                        countDistinct(col(PRODUCT_EXTERNAL_ID)).alias(DIFF_PRODUCTS_NUMBER_FIELD)
                );
        // handling the case when there were no sales in some week, and it disappears from the weekData
        List<Row> weekDataList = weekData.collectAsList();

        Timestamp startDateTime = Timestamp.valueOf(currentDay.minusYears(1));

        if (!weekDataList.isEmpty()) {
            startDateTime = new SimpleRowWrapper(weekData.sort(col(WEEK_DATE_FIELD)).collectAsList().get(0)).getTimestamp(WEEK_DATE_FIELD);
        }

        Timestamp endDateTime = Timestamp.valueOf(currentDay.minusWeeks(1));

        Dataset<Timestamp> startWeeksDataset = sparkDataService.createOneColumnDataset(sparkSession,
                generateWeekTimeKeys(startDateTime.toLocalDateTime(), endDateTime.toLocalDateTime()), Encoders.TIMESTAMP());

        Dataset<Row> resultStartWeeksDataset = startWeeksDataset.withColumnRenamed(VALUE_FIELD, WEEK_DATE_FIELD);

        return weekData
                .join(resultStartWeeksDataset, toSeq(Collections.singletonList(WEEK_DATE_FIELD)), "right")
                .withColumn(ORDER_NUMBER_FIELD, coalesce(col(ORDER_NUMBER_FIELD), lit(0L)))
                .withColumn(CUSTOMER_NUMBER_FIELD, coalesce(col(CUSTOMER_NUMBER_FIELD), lit(0L)))
                .withColumn(ITEMS_NUMBER_FIELD, coalesce(col(ITEMS_NUMBER_FIELD), lit(0L)))
                .withColumn(TOTAL_VALUE_FIELD, coalesce(col(TOTAL_VALUE_FIELD), lit(0D)))
                .withColumn(DIFF_PRODUCTS_NUMBER_FIELD, coalesce(col(DIFF_PRODUCTS_NUMBER_FIELD), lit(0L)));
    }

    private String getJobName() {
        return "Data Processing Job";
    }
}
