package dpl.processing.job;

import dpl.processing.model.*;
import dpl.processing.job.context.ProcessJobContext;
import dpl.processing.service.AggregatedDataService;
import dpl.processing.service.spark.IPostgresSparkDataService;
import dpl.processing.utils.ProductUtils;
import dpl.processing.vo.YearChartPoint;
import dpl.processing.vo.holder.ForecastCalculationHolder;
import dpl.processing.vo.holder.ProductDataHolder;
import dpl.processing.vo.wrapper.row.EmptyRowWrapper;
import dpl.processing.vo.wrapper.row.RowWrapper;
import dpl.processing.vo.wrapper.row.SimpleRowWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.collection.Seq;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static dpl.processing.constants.PostgresConstants.*;
import static dpl.processing.utils.DateUtils.*;
import static dpl.processing.utils.ProductUtils.FIRST_ORDER_ID_FIELD;
import static dpl.processing.utils.ScalaUtils.toSeq;
import static java.time.temporal.TemporalAdjusters.firstDayOfYear;
import static org.apache.spark.sql.functions.*;

@Slf4j
@Component
public class DataProcessingJob {
    protected static final String WEEK_DATE_FIELD = "week_date";
    protected static final String DAY_DATE_FIELD = "day_date";

    protected static final String OLDEST_ORDER_TIMESTAMP_FIELD = "oldest_order_timestamp_5y_cut";
    protected static final String OLDEST_ORDER_TIMESTAMP_FIELD_W_CUT = "oldest_order_timestamp";
    protected static final String JOB_DATE_FIELD = "job_date";
    public static final String TOTAL_VALUE_FIELD = "total_value";
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

    private static final String WEEK_DAY_NUMBER = "day_of_week";
    private static final String WEEK_NUMBER_FIELD = "week_number";
    private static final String FORECAST_FIELD = "forecast";

    private static final String SPENT_RANK_FIELD = "spent_rank";
    private static final String ACTIVE_CUSTOMERS_VALUE = "active_customers_last_year_value";
    private static final String TOTAL_CUSTOMERS_VALUE = "total_customers_last_year_value";
    private static final String ACTIVE_CUSTOMERS = "active_customers";
    private static final String AT_RISK_CUSTOMERS_VALUE = "at_risk_customers_last_year_value";
    private static final String AVG_CUSTOMER_SPEND_VALUE = "avg_customer_spend_value";
    private static final String PRODUCT_IDS = "product_ids";
    private static final String FIRST_PRODUCT_IDS = "first_product_ids";
    private static final String ONE_ORDER_CUSTOMERS = "one_order_customers";
    private static final String TOTAL_ORDER_NUMBER = "total_order_number";
    private static final String ORDER_FREQUENCY_FIELD = "order_frequency";
    private static final String ORDER_AVERAGE_VALUE_FIELD = "order_average_value";

    protected static final String LEFT_JOIN = "left";
    protected static final Seq<String> USER_ID_TO_JOIN = toSeq(Collections.singletonList(USER_ID_FIELD));
    protected static final Seq<String> ORDER_CUSTOMER_ID_TO_JOIN = toSeq(Collections.singletonList(ORDER_CUSTOMER_ID_FIELD));
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
                .where(col(ORDER_TIMESTAMP).lt(Timestamp.valueOf(jobDate))
                        .and(upper(col(ORDER_PAYMENT_STATUS)).isin(ORDER_PAID_STATUSES.toArray())))
                .withColumn(WEEK_DATE_FIELD, date_trunc("WEEK", col(ORDER_TIMESTAMP)))
                .withColumn(TOTAL_VALUE_FIELD, coalesce(col(LINE_VALUE_INCLUDING_TAX), col(LINE_VALUE_EXCLUDING_TAX), lit(0))
                                .multiply(coalesce(col(ORDER_LINE_ITEM_QTY), lit(1))).cast(DataTypes.DoubleType)
                );

        if (log.isTraceEnabled()) {
            log.trace("{} job: loaded ORDER dataset with {} rows", getJobName(), ordersDataSet.count());
        }

        AggregatedData aggregatedData = startProcessingData(context, sessionName, jobDate, ordersDataSet);

        dataService.saveData(aggregatedData);
    }

    private AggregatedData startProcessingData(ProcessJobContext context, String sparkSession, LocalDateTime jobDate, Dataset<Row> ordersDataSet) {
        AggregatedData aggregatedData = new AggregatedData();
        Column timestampColumn = col(ORDER_TIMESTAMP);

        log.trace("{} job: generating card header", getJobName());

        Dataset<Row> avgByDayOfWeekData = ordersDataSet
                .filter(timestampColumn.$greater$eq(Timestamp.valueOf(jobDate.minusDays(7))))
                .withColumn(DAY_DATE_FIELD, date_trunc("DAY", col(ORDER_TIMESTAMP)))
                .groupBy(DAY_DATE_FIELD)
                .agg(
                        sum(TOTAL_VALUE_FIELD).cast(DataTypes.DoubleType).as(AVG_BY_DAY_WEEK_FIELD)
                )
                .select(avg(col(AVG_BY_DAY_WEEK_FIELD)).as(AVG_BY_DAY_WEEK_FIELD));

        BigDecimal last7daysAverageSales = BigDecimal.valueOf(new SimpleRowWrapper(avgByDayOfWeekData
                .collectAsList().get(0)).getDouble(AVG_BY_DAY_WEEK_FIELD, 0D));
        SimpleRowWrapper headerData = new SimpleRowWrapper(ordersDataSet.agg(

                        sum(col(TOTAL_VALUE_FIELD)).as(TOTAL_VALUE_FIELD),
                        sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(jobDate.minusYears(1))), col(TOTAL_VALUE_FIELD)))
                                .as(TOTAL_VALUE_YEAR_FIELD),
                        sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(jobDate.minusDays(7))), col(TOTAL_VALUE_FIELD)))
                                .as(TOTAL_VALUE_WEEK_FIELD),
                        sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(jobDate.minusDays(1))), col(TOTAL_VALUE_FIELD)))
                                .as(YESTERDAY_TOTAL_VALUE_FIELD),

                        countDistinct(when(timestampColumn.$greater$eq(Timestamp.valueOf(jobDate.minusYears(1))), col(ORDER_ID_FIELD)))
                                .alias(ORDER_NUMBER_FIELD),
                        countDistinct(when(timestampColumn.$greater$eq(Timestamp.valueOf(jobDate.minusDays(7))), col(ORDER_ID_FIELD)))
                                .alias(LAST_WEEK_ORDER_NUMBER_FIELD),
                        countDistinct(when(timestampColumn.$greater$eq(Timestamp.valueOf(jobDate.minusDays(1))), col(ORDER_ID_FIELD)))
                                .alias(YESTERDAY_ORDER_NUMBER_FIELD)
                ).withColumn(AVG_ORDER_VALUE_FIELD, col(TOTAL_VALUE_FIELD).divide(col(ORDER_NUMBER_FIELD)).cast(DataTypes.DoubleType))
                .withColumn(LAST_WEEK_ANG_ORDER_VALUE_FIELD, col(TOTAL_VALUE_WEEK_FIELD).divide(col(LAST_WEEK_ORDER_NUMBER_FIELD)).cast(DataTypes.DoubleType))
                .withColumn(YESTERDAY_ANG_ORDER_VALUE_FIELD, col(YESTERDAY_TOTAL_VALUE_FIELD).divide(col(YESTERDAY_ORDER_NUMBER_FIELD)).cast(DataTypes.DoubleType))
                .collectAsList().get(0));

        LocalDateTime lastWeek = jobDate.minusDays(7);
        LocalDate yearStart = LocalDate.now().with(firstDayOfYear());

        ForecastCalculationHolder forecastCalculationHolder = new ForecastCalculationHolder(
                getWeekData(sparkSession, jobDate, ordersDataSet)
                        .sort(col(WEEK_DATE_FIELD).desc())
                        .limit(52),
                jobDate, yearStart);

        List<YearChartPoint> yearData = forecastCalculationHolder.getYearData();
        List<YearChartPoint> predictions = forecastCalculationHolder.getPredictions();
        Dataset<Row> weekData = forecastCalculationHolder.getWeekData();

        // day-by-day predictions
        log.trace("{} job : Starting calculating predictions for last 6 weeks", getJobName());

        Dataset<Row> last6weeksOrders = ordersDataSet.where(timestampColumn.$greater$eq(Timestamp.valueOf(jobDate.minusWeeks(6))));

        List<Row> last6WeeksData = last6weeksOrders.select(sum(col(TOTAL_VALUE_FIELD)).as(TOTAL_VALUE_FIELD)).collectAsList();

        BigDecimal last6weeksTotal = last6WeeksData.isEmpty() ? BigDecimal.ZERO : BigDecimal.valueOf(new SimpleRowWrapper(last6WeeksData.get(0))
                .getDouble(TOTAL_VALUE_FIELD, 0.0));

        log.trace("{} job : Total for last 6 weeks is {}", getJobName(), last6weeksTotal);

        Map<Integer, DayData> dataByDays = last6weeksOrders
                .groupBy(dayofweek(timestampColumn).as(WEEK_DAY_NUMBER))
                .agg(sum(col(TOTAL_VALUE_FIELD)).as(TOTAL_VALUE_FIELD))
                .orderBy(WEEK_DAY_NUMBER)
                .collectAsList()
                .stream()
                .map(SimpleRowWrapper::new)
                .collect(HashMap::new, (map, wr) -> map.put(wr.getInt(WEEK_DAY_NUMBER),
                                new DayData(BigDecimal.ZERO.compareTo(last6weeksTotal) == 0
                                        ? BigDecimal.ZERO
                                        : BigDecimal.valueOf(wr.getDouble(TOTAL_VALUE_FIELD, 0.0))
                                        .multiply(predictions.get(0).getForecast())
                                        .divide(last6weeksTotal, 2, RoundingMode.HALF_UP))),
                        HashMap::putAll);

        WeekData shopifyWeekData = new WeekData(
                dataByDays.getOrDefault(DayOfWeek.MONDAY.getValue(), DayData.EMPTY_FORECAST),
                dataByDays.getOrDefault(DayOfWeek.TUESDAY.getValue(), DayData.EMPTY_FORECAST),
                dataByDays.getOrDefault(DayOfWeek.WEDNESDAY.getValue(), DayData.EMPTY_FORECAST),
                dataByDays.getOrDefault(DayOfWeek.THURSDAY.getValue(), DayData.EMPTY_FORECAST),
                dataByDays.getOrDefault(DayOfWeek.FRIDAY.getValue(), DayData.EMPTY_FORECAST),
                dataByDays.getOrDefault(DayOfWeek.SATURDAY.getValue(), DayData.EMPTY_FORECAST),
                dataByDays.getOrDefault(DayOfWeek.SUNDAY.getValue(), DayData.EMPTY_FORECAST)
        );


        if (log.isTraceEnabled()) {
            log.trace("{} job: Loaded predictions for each days in current week", getJobName());
            StringBuilder valuesForDays = new StringBuilder();
            shopifyWeekData.getDaysList().forEach(dayData -> valuesForDays.append(dayData.getForecast()).append(" "));
            log.trace("{} job: Predictions for days: {}", getJobName(), valuesForDays);
        }

        log.trace("{} job : Loaded predictions for last 6 weeks", getJobName());

        // new customers
        long newCustomersCount = ordersDataSet.groupBy(ORDER_CUSTOMER_ID_FIELD)
                .agg(countDistinct(ID_FIELD).as(ORDER_NUMBER_FIELD),
                        countDistinct(when(timestampColumn.$greater(Timestamp.valueOf(lastWeek)), ID_FIELD)).as(LAST_WEEK_ORDER_NUMBER_FIELD)
                )
                .filter(col(ORDER_NUMBER_FIELD).equalTo(col(LAST_WEEK_ORDER_NUMBER_FIELD)))
                .count();

        log.trace("{} job : Find customer count: {}", getJobName(), newCustomersCount);

        // construction of week stats
        Row lastWeekInfoRow = weekData.orderBy(col(WEEK_NUMBER_FIELD).desc()).collectAsList().get(0);

        RowWrapper<String> lastWeekInfo = new EmptyRowWrapper();

        if (lastWeekInfoRow != null) {
            lastWeekInfo = new SimpleRowWrapper(lastWeekInfoRow);
        }

        List<Row> weekYearAgo = weekData
                .filter(col(WEEK_DATE_FIELD).equalTo(Timestamp.valueOf(jobDate.minusWeeks(52))))
                .collectAsList();

        RowWrapper<String> weekYearAgoInfo = weekYearAgo.isEmpty() ? new EmptyRowWrapper() : new SimpleRowWrapper(weekYearAgo.get(0));

        List<Row> last52Weeks = weekData
                .orderBy(col(WEEK_NUMBER_FIELD).desc())
                .limit(52)
                .agg(sum(TOTAL_VALUE_FIELD).divide(sum(ORDER_NUMBER_FIELD)).as(AVG_ORDER_VALUE_FIELD))
                .collectAsList();

        BigDecimal lastYearAverageOrderValue = last52Weeks.isEmpty() ? BigDecimal.ZERO :
                round(BigDecimal.valueOf(new SimpleRowWrapper(last52Weeks.get(0)).getDouble(AVG_ORDER_VALUE_FIELD, 0.0)));

        int financialWeek = weekNumberInFinancialYear(yearStart, jobDate.toLocalDate());

        List<BigDecimal> last20weeks = forecastCalculationHolder.getLast20weeksInfo();
        BigDecimal current = last20weeks.isEmpty() ? BigDecimal.ZERO : last20weeks.get(0);
        last20weeks.sort(Collections.reverseOrder());

        List<QuarterData> quarterData = forecastCalculationHolder.getQuarterData();



        aggregatedData.setTotalSales(toBigDecimal(headerData.getDouble(TOTAL_VALUE_YEAR_FIELD, 0.0)));
        aggregatedData.setYesterdaySales(toBigDecimal(headerData.getDouble(YESTERDAY_TOTAL_VALUE_FIELD, 0.0)));
        aggregatedData.setYesterdayAverageOrderValue(toBigDecimal(headerData.getDouble(YESTERDAY_ANG_ORDER_VALUE_FIELD, 0.0)));
        aggregatedData.setLast7daysAverageSales(last7daysAverageSales);
        aggregatedData.setLast7daysAverageOrderValue(toBigDecimal(headerData.getDouble(LAST_WEEK_ANG_ORDER_VALUE_FIELD, 0.0)));
        aggregatedData.setLast20weeksRanking(last20weeks.indexOf(current) + 1);
        aggregatedData.setLast20weeksValues(last20weeks);
        aggregatedData.setWeekData(shopifyWeekData);
        aggregatedData.setLastWeekOrders(lastWeekInfo.getLong(ORDER_NUMBER_FIELD, 0L));
        aggregatedData.setLastWeekCustomers(lastWeekInfo.getLong(CUSTOMER_NUMBER_FIELD, 0L));
        aggregatedData.setLastWeekNewCustomers(newCustomersCount);
        aggregatedData.setLastWeekAverageOrderValue(round(BigDecimal.valueOf(lastWeekInfo.getDouble(AVG_ORDER_VALUE_FIELD, 0.0))));
        aggregatedData.setSalesLastWeek(round(BigDecimal.valueOf(lastWeekInfo.getDouble(TOTAL_VALUE_FIELD, 0.0))));
        aggregatedData.setSalesLastYearWeek(round(BigDecimal.valueOf(weekYearAgoInfo.getDouble(TOTAL_VALUE_FIELD, 0.0))));
        aggregatedData.setForecastLastYearWeek(round(BigDecimal.valueOf(weekYearAgoInfo.getDouble(FORECAST_FIELD, 0.0))));
        aggregatedData.setQuarter0(quarterData.get(0));
        aggregatedData.setQuarter1(quarterData.get(1));
        aggregatedData.setQuarter2(quarterData.get(2));
        aggregatedData.setQuarter3(quarterData.get(3));
        aggregatedData.setYearData(new YearData(yearData));
        aggregatedData.setYearForecast(forecastCalculationHolder.getYearForecast());
        aggregatedData.setLastYearAverageOrderValue(lastYearAverageOrderValue);

        
        Dataset<Row> customerStatsDataset = createCustomerStatsDataset(jobDate, ordersDataSet)
                .withColumn(SPENT_RANK_FIELD, percent_rank().over(Window.orderBy(CUSTOMER_TOTAL_VALUE_FIELD)))
                .cache();

        log.trace("{} job: Constructed CUSTOMER dataset", getJobName());

        SimpleRowWrapper additionalCustomersInfo = new SimpleRowWrapper(
                customerStatsDataset
                        .agg(
                                countDistinct(when(col(IS_ACTIVE_FIELD).equalTo(lit(true)), col(USER_ID_FIELD)))
                                        .alias(ACTIVE_CUSTOMERS),
                                countDistinct(when(col(IS_AT_RISK_FIELD).equalTo(lit(true)), col(USER_ID_FIELD)))
                                        .alias(RISK_OF_LEAVING_CUSTOMERS),
                                sum(when(col(IS_ACTIVE_FIELD).equalTo(lit(true)), col(TOTAL_VALUE_YEAR_FIELD)).otherwise(lit(0)))
                                        .alias(ACTIVE_CUSTOMERS_VALUE),
                                sum(col(TOTAL_VALUE_YEAR_FIELD)).alias(TOTAL_CUSTOMERS_VALUE),
                                sum(when(col(IS_AT_RISK_FIELD).equalTo(lit(true)), col(TOTAL_VALUE_YEAR_FIELD)).otherwise(lit(0)))
                                        .alias(AT_RISK_CUSTOMERS_VALUE)
                        ).collectAsList().get(0)
        );

        log.trace("{} job: Aggregated and loaded CUSTOMER data", getJobName());

        log.trace("{} job: Constructed CUSTOMER with ENGAGEMENT dataset", getJobName());

        Dataset<Row> productsDataset = sparkDataService.loadProductDataForOrg(sparkSession, context);

        log.trace("{} job: Constructed PRODUCT dataset", getJobName());

        if (log.isTraceEnabled()) {
            log.trace("{} job: Size of PRODUCT dataset is {} rows", getJobName(), productsDataset.count());
        }

        log.trace("{} job: Building Shopify Card Stage", getJobName());

        SegmentData highValueSegmentData = aggregateSegmentData(ordersDataSet, productsDataset, customerStatsDataset,
                jobDate, additionalCustomersInfo, SegmentType.HIGH_VALUE);

        SegmentData averageValueSegmentData = aggregateSegmentData(ordersDataSet, productsDataset, customerStatsDataset,
                jobDate, additionalCustomersInfo, SegmentType.AVERAGE_VALUE);

        SegmentData lowValueSegmentData = aggregateSegmentData(ordersDataSet, productsDataset, customerStatsDataset,
                jobDate, additionalCustomersInfo, SegmentType.LOW_VALUE);

        customerStatsDataset.unpersist();

        aggregatedData.setActiveCustomers(additionalCustomersInfo.getLong(ACTIVE_CUSTOMERS, 0L));
        aggregatedData.setAtRiskCustomers(additionalCustomersInfo.getLong(RISK_OF_LEAVING_CUSTOMERS, 0L));
        aggregatedData.setActiveCustomersLastYearSpend(BigDecimal.valueOf(additionalCustomersInfo.getDouble(ACTIVE_CUSTOMERS_VALUE, 0.0)));
        aggregatedData.setAtRiskCustomersLastYearSpend(BigDecimal.valueOf(additionalCustomersInfo.getDouble(AT_RISK_CUSTOMERS_VALUE, 0.0)));
        aggregatedData.setHighValueSegmentData(highValueSegmentData);
        aggregatedData.setAverageValueSegmentData(averageValueSegmentData);
        aggregatedData.setLowValueSegmentData(lowValueSegmentData);

        return aggregatedData;
    }

    protected Dataset<Row> createCustomerStatsDataset(LocalDateTime jobDate, Dataset<Row> ordersDataSet) {
        log.trace("{} job: loading user stats dataset", getJobName());

        Column timestampColumn = col(ORDER_TIMESTAMP);

        Dataset<Row> dates = ordersDataSet.groupBy(ORDER_CUSTOMER_ID_FIELD)
                .agg(
                        min(timestampColumn).as(OLDEST_ORDER_TIMESTAMP_FIELD_W_CUT),
                        when(min(timestampColumn).gt(Timestamp.valueOf(jobDate.minusYears(5))), min(timestampColumn))
                                .otherwise(Timestamp.valueOf(jobDate.minusYears(5))).as(OLDEST_ORDER_TIMESTAMP_FIELD),
                        lit(Timestamp.valueOf(jobDate)).as(JOB_DATE_FIELD)
                )
                .withColumn(QUARTER2_LOWER_BOUND, expr("date_sub(job_date, cast(datediff(job_date, oldest_order_timestamp) / 4 as int))").cast("timestamp"))
                .withColumn(QUARTER3_LOWER_BOUND, expr("date_sub(job_date, cast(datediff(job_date, oldest_order_timestamp) / 2 as int))").cast("timestamp"))
                .withColumn(QUARTER4_LOWER_BOUND, expr("date_sub(job_date, cast(datediff(job_date, oldest_order_timestamp) * 3 / 4 as int))").cast("timestamp"));


        if (log.isTraceEnabled()) {
            log.trace("{} job: loaded CUSTOMER data with {} rows", getJobName(), dates.count());
        }

        WindowSpec window = Window.partitionBy(ID_FIELD).orderBy(col(ORDER_TIMESTAMP).asc_nulls_last());

        return ordersDataSet
                .withColumn(FIRST_ORDER_ID_FIELD, first(col(ID_FIELD), true).over(window))
                .join(dates, ORDER_CUSTOMER_ID_TO_JOIN, LEFT_JOIN)
                .groupBy(col(ID_FIELD))
                .agg(
                        first(FIRST_ORDER_ID_FIELD).alias(FIRST_ORDER_ID_FIELD),
                        countDistinct(col(ID_FIELD)).alias(ORDER_NUMBER_FIELD),
                        coalesce(sum(col(TOTAL_VALUE_FIELD)), lit(0)).as(CUSTOMER_TOTAL_VALUE_FIELD),
                        coalesce(sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(jobDate.minusYears(1))), col(TOTAL_VALUE_FIELD))), lit(0))
                                .as(TOTAL_VALUE_YEAR_FIELD),
                        coalesce(sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(jobDate.minusDays(90))), col(TOTAL_VALUE_FIELD))), lit(0))
                                .as(TOTAL_VALUE_QUARTER_FIELD),
                        coalesce(sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(jobDate.minusDays(90))), col(TOTAL_VALUE_FIELD))), lit(0))
                                .as(TOTAL_VALUE_90_DAYS_FIELD),
                        coalesce(sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(jobDate.minusDays(60))), col(TOTAL_VALUE_FIELD))), lit(0))
                                .as(TOTAL_VALUE_60_DAYS_FIELD),
                        coalesce(sum(when(timestampColumn.$greater$eq(Timestamp.valueOf(jobDate.minusDays(30))), col(TOTAL_VALUE_FIELD))), lit(0))
                                .as(TOTAL_VALUE_30_DAYS_FIELD),
                        countDistinct(when(timestampColumn.lt(col(QUARTER2_LOWER_BOUND)), when(timestampColumn.$greater$eq(col(OLDEST_ORDER_TIMESTAMP_FIELD)), col(ID_FIELD)))).as(ORDERS_COUNT_QUARTER1),
                        countDistinct(when(timestampColumn.lt(col(QUARTER3_LOWER_BOUND)), when(timestampColumn.$greater$eq(col(QUARTER2_LOWER_BOUND)), col(ID_FIELD)))).as(ORDERS_COUNT_QUARTER2),
                        countDistinct(when(timestampColumn.lt(col(QUARTER4_LOWER_BOUND)), when(timestampColumn.$greater$eq(col(QUARTER3_LOWER_BOUND)), col(ID_FIELD)))).as(ORDERS_COUNT_QUARTER3),
                        countDistinct(when(timestampColumn.$less$eq(col(JOB_DATE_FIELD)), when(timestampColumn.$greater$eq(col(QUARTER4_LOWER_BOUND)), col(ID_FIELD)))).as(ORDERS_COUNT_QUARTER4),
                        first(col(OLDEST_ORDER_TIMESTAMP_FIELD_W_CUT), true).as(OLDEST_ORDER_TIMESTAMP_FIELD_W_CUT),
                        first(col(OLDEST_ORDER_TIMESTAMP_FIELD), true).as(OLDEST_ORDER_TIMESTAMP_FIELD),
                        first(col(JOB_DATE_FIELD), true).as(JOB_DATE_FIELD),
                        first(col(ORDER_CUSTOMER_ID_FIELD), true).as(USER_ID_FIELD)
                )
                .withColumn(IS_LOYAL_FIELD, col(ORDERS_COUNT_QUARTER1).plus(col(ORDERS_COUNT_QUARTER3)).$greater(lit(0))
                        .and(col(ORDERS_COUNT_QUARTER2).plus(col(ORDERS_COUNT_QUARTER4)).$greater(lit(0))))
                .withColumn(IS_ACTIVE_FIELD, col(TOTAL_VALUE_90_DAYS_FIELD).$greater(lit(0)))
                .withColumn(IS_AT_RISK_FIELD, col(CUSTOMER_TOTAL_VALUE_FIELD).$greater(col(TOTAL_VALUE_YEAR_FIELD))
                        .and(col(TOTAL_VALUE_YEAR_FIELD).$greater(col(TOTAL_VALUE_60_DAYS_FIELD)))
                        .and(col(TOTAL_VALUE_60_DAYS_FIELD).equalTo(lit(0)))
                );

    }

    private SegmentData aggregateSegmentData(Dataset<Row> ordersDataSet,
                                             Dataset<Row> productsDataset, Dataset<Row> segmentData,
                                             LocalDateTime currentDay, SimpleRowWrapper additionalCustomersInfo,
                                             SegmentType segmentType) {
        log.trace("{} job: Aggregating data for {} Segment", getJobName(), segmentType);
        Dataset<Row> filteredSegment = segmentData
                .filter(segmentType.getBuildFilterColumn().apply(SPENT_RANK_FIELD))
                .filter(col(IS_ACTIVE_FIELD).equalTo(lit(true)));
        long segmentCustomers = filteredSegment.count();

        log.trace("{} job: {} Segment has {} customers", getJobName(), segmentType, segmentCustomers);
        long totalCustomers = segmentData.count();

        log.trace("{} job: Segment {} has {}/{} CUSTOMERS", getJobName(), segmentType, segmentCustomers, totalCustomers);

        SimpleRowWrapper productIdsWrapper = ProductUtils.getProductIdsWrapper(ordersDataSet, productsDataset, filteredSegment);

        List<ProductDataHolder> products = ProductUtils.sortProductInfo(productIdsWrapper.getListOfStructs(PRODUCT_IDS));
        List<ProductDataHolder> firstProducts = ProductUtils.sortProductInfo(productIdsWrapper.getListOfStructs(FIRST_PRODUCT_IDS));

        log.trace("{} job: Segment {} customers bought {} products, {} in the first order", getJobName(), segmentType,
                products.size(), firstProducts.size());

        List<ProductData> top5Products = ProductUtils.findTopProduct(segmentCustomers, products, 5);

        ProductData leadingPurchase = ProductUtils.getLeadingPurchase(segmentCustomers, firstProducts);

        if (leadingPurchase == null) {
            log.trace("{} job: Segment {} CUSTOMERS don't have any purchases", getJobName(), segmentType);
        } else {
            log.trace("{} job: Segment {} leading product is '{}'", getJobName(), segmentType, leadingPurchase.getName());
        }

        Dataset<Row> aggregatedDataset = filteredSegment
                .withColumn(ORDER_FREQUENCY_FIELD, unix_timestamp(col(JOB_DATE_FIELD)).minus(unix_timestamp(col(OLDEST_ORDER_TIMESTAMP_FIELD_W_CUT)))
                        .divide(col(ORDER_NUMBER_FIELD)))
                .withColumn(ORDER_AVERAGE_VALUE_FIELD, col(CUSTOMER_TOTAL_VALUE_FIELD).divide(col(ORDER_NUMBER_FIELD)))
                .select(
                        countDistinct(when(col(IS_ACTIVE_FIELD).equalTo(lit(true)), col(USER_ID_FIELD)))
                                .alias(ACTIVE_CUSTOMERS),
                        countDistinct(when(col(ORDER_NUMBER_FIELD).equalTo(lit(1)), col(USER_ID_FIELD)))
                                .alias(ONE_ORDER_CUSTOMERS),
                        sum(ORDER_NUMBER_FIELD).alias(TOTAL_ORDER_NUMBER),
                        sum(CUSTOMER_TOTAL_VALUE_FIELD).alias(CUSTOMER_TOTAL_VALUE_FIELD),
                        sum(when(col(IS_ACTIVE_FIELD).equalTo(lit(true)), col(TOTAL_VALUE_YEAR_FIELD)).otherwise(lit(0)))
                                .alias(ACTIVE_CUSTOMERS_VALUE),
                        avg(col(ORDER_AVERAGE_VALUE_FIELD)).as(AVG_ORDER_VALUE_FIELD),
                        avg(col(CUSTOMER_TOTAL_VALUE_FIELD)).as(AVG_CUSTOMER_SPEND_VALUE),
                        avg(col(ORDER_FREQUENCY_FIELD)).as(ORDER_FREQUENCY_FIELD)
                );

        // segment history
        List<ChartPoint> segmentChartDataHistory = getSegmentChartDataHistory(ordersDataSet, currentDay, segmentType, segmentData);
        ChartPoint currentPoint = new ChartPoint(asDate(currentDay), BigDecimal.valueOf(filteredSegment.count()));
        segmentChartDataHistory.add(currentPoint);
        segmentChartDataHistory.sort(Comparator.comparing(ChartPoint::getDate));

        log.trace("{} job: Built Segment {} history has {} history points", getJobName(), segmentType, segmentChartDataHistory.size());

        SimpleRowWrapper aggregatedSegmentInfo = new SimpleRowWrapper(aggregatedDataset.collectAsList().get(0));

        log.trace("{} job: Building {} Segment Entity", getJobName(), segmentType);

        return SegmentData.builder()
                .numberOfCustomers(segmentCustomers)
                .averageSpend(toBigDecimal(aggregatedSegmentInfo.getDouble(AVG_CUSTOMER_SPEND_VALUE, 0.0)))
                .activeCustomers(aggregatedSegmentInfo.getLong(ACTIVE_CUSTOMERS, 0L))
                .allActiveCustomers(additionalCustomersInfo.getLong(ACTIVE_CUSTOMERS, 0L))
                .totalRevenueCustomers(BigDecimal.valueOf(additionalCustomersInfo.getDouble(TOTAL_CUSTOMERS_VALUE, 0.0)))
                .revenueCustomers(BigDecimal.valueOf(aggregatedSegmentInfo.getDouble(ACTIVE_CUSTOMERS_VALUE, 0.0)))
                .oneOrderCustomers(aggregatedSegmentInfo.getLong(ONE_ORDER_CUSTOMERS, 0L))
                .totalOrderNumber(aggregatedSegmentInfo.getLong(TOTAL_ORDER_NUMBER, 0L))
                .averageOrderValue(toBigDecimal(aggregatedSegmentInfo.getDouble(AVG_ORDER_VALUE_FIELD, 0.0)))
                .averagePurchaseFrequency(toBigDecimal(aggregatedSegmentInfo.getDouble(ORDER_FREQUENCY_FIELD, 0.0)))
                .averageVisitFrequencySeconds(toBigDecimal(aggregatedSegmentInfo.getDouble(ENGAGEMENT_GAP_SECONDS_FIELD, 0.0)))
                .mostPopularProducts(top5Products)
                .leadingPurchase(leadingPurchase)
                .chartData(segmentChartDataHistory)
                .build();
    }

    private List<ChartPoint> getSegmentChartDataHistory(Dataset<Row> ordersDataSet, LocalDateTime currentDay,
                                                        SegmentType segmentType, Dataset<Row> segmentData) {

        return LongStream.range(1, 10)
                .mapToObj(currentDay::minusWeeks)
                .map(day -> new ChartPoint(
                        asDate(day),
                        BigDecimal.valueOf(ordersDataSet.where(col(ORDER_TIMESTAMP).lt(Timestamp.valueOf(day)))
                                .join(segmentData, segmentData.col(USER_ID_FIELD)
                                        .equalTo(ordersDataSet.col(USER_ID_FIELD)), LEFT_JOIN)
                                .groupBy(ordersDataSet.col(USER_ID_FIELD))
                                .agg(
                                        coalesce(sum(col(TOTAL_VALUE_FIELD)), lit(0)).as(CUSTOMER_TOTAL_VALUE_FIELD),
                                        first(col(IS_ACTIVE_FIELD)).as(IS_ACTIVE_FIELD)
                                )
                                .withColumn(SPENT_RANK_FIELD, percent_rank().over(Window.orderBy(CUSTOMER_TOTAL_VALUE_FIELD)))
                                .where(segmentType.getBuildFilterColumn().apply(SPENT_RANK_FIELD))
                                .filter(col(IS_ACTIVE_FIELD).equalTo(lit(true)))
                                .count())))
                .collect(Collectors.toList());
    }


    public static BigDecimal toBigDecimal(double value) {
        return round(BigDecimal.valueOf(value));
    }

    public static BigDecimal round(BigDecimal value) {
        return value.setScale(2, RoundingMode.HALF_UP);
    }

    protected Dataset<Row> getWeekData(String sparkSession, LocalDateTime jobDate, Dataset<Row> ordersDataSet) {
        // data for all weeks
        Dataset<Row> weekData = ordersDataSet.groupBy(WEEK_DATE_FIELD)
                .agg(
                        countDistinct(col(ID_FIELD)).alias(ORDER_NUMBER_FIELD),
                        countDistinct(USER_ID_FIELD).alias(CUSTOMER_NUMBER_FIELD),
                        sum(col(ORDER_LINE_ITEM_QTY)).alias(ITEMS_NUMBER_FIELD),
                        sum(TOTAL_VALUE_FIELD).alias(TOTAL_VALUE_FIELD),
                        countDistinct(col(ORDER_PRODUCT_ID_FIELD)).alias(DIFF_PRODUCTS_NUMBER_FIELD)
                );
        // handling the case when there were no sales in some week, and it disappears from the weekData
        List<Row> weekDataList = weekData.collectAsList();

        Timestamp startDateTime = Timestamp.valueOf(jobDate.minusYears(1));

        if (!weekDataList.isEmpty()) {
            startDateTime = new SimpleRowWrapper(weekData.sort(col(WEEK_DATE_FIELD)).collectAsList().get(0)).getTimestamp(WEEK_DATE_FIELD);
        }

        Timestamp endDateTime = Timestamp.valueOf(jobDate.minusWeeks(1));

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
