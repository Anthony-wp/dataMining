package dpl.processing.vo.holder;

import dpl.processing.model.QuarterData;
import dpl.processing.vo.YearChartPoint;
import dpl.processing.vo.wrapper.row.SimpleRowWrapper;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static dpl.processing.job.DataProcessingJob.*;
import static dpl.processing.utils.DateUtils.asDate;
import static dpl.processing.utils.DateUtils.setYearStartRelatively;
import static org.apache.spark.sql.functions.*;

@Slf4j
@NoArgsConstructor
@Getter
public class ForecastCalculationHolder {
    protected static final String WEEK_DATE_FIELD = "week_date";

    private static final double C_CONSTANT_FOR_PREDICTION_INTERVALS = 1.28;
    private static final int GRAPH_WEEKS_COUNT = 52;
    private static final int WEEKS_FOR_FORECAST = 52;

    private static final String CUSTOMER_NUMBER_FIELD = "num_of_customers";
    private static final String YEAR_WEEK_NUMBER = "week_of_year";
    private static final String WEEK_NUMBER_FIELD = "week_number";
    private static final String WEEK_TREND_VALUE = "week_trend_value";
    private static final String ACTUAL_DIFF_VALUE = "actual_diff";
    private static final String ACTUAL_MULTIPLY_VALUE = "actual_multiply";
    private static final String WEEK_DIFF_AVG = "avg_week_diff";
    private static final String STD_DEV_FIELD = "std_dev";
    private static final String FORECAST_FIELD = "forecast";
    private static final String WEEK_SEASONALITY_VALUE_FIELD = "week_seasonality_value";

    private static final String ROLLING_AVG_VALUE_FIELD = "rolling_4w_value";
    private static final String CENTRED_AVG_VALUE_FIELD = "centred_average";
    private static final String CURRENT_DAY_FIELD = "current_day";
    private static final String XY_VALUE_FIELD = "xy";
    private static final String X_SUM_FIELD = "x_sum";
    private static final String X_POW_2_FIELD = "x_pov_2";
    private static final String XY_SUM_FIELD = "xy_sum";
    private static final String X_POW_2_SUM_FIELD = "x_pov_2_sum";
    private static final String Y_SUM_FIELD = "y_sum";
    private static final String M_VALUE_FIELD = "m";
    private static final String B_VALUE_FIELD = "b";

    private List<YearChartPoint> yearData;
    private List<BigDecimal> last20weeksInfo;
    private List<YearChartPoint> predictions;
    private List<YearChartPoint> existingData;
    private List<QuarterData> quarterData;
    private Dataset<Row> weekData;

    public ForecastCalculationHolder(Dataset<Row> weekData, LocalDateTime currentDay,
                                     LocalDate configuredYearStart, BigDecimal weekForecast) {
        // window over the all order dataset...
        WindowSpec weekWindow = Window.partitionBy().orderBy(WEEK_DATE_FIELD);
        this.weekData = weekData;

        this.weekData = this.weekData
                .withColumn(AVG_ORDER_VALUE_FIELD, col(TOTAL_VALUE_FIELD).divide(col(ORDER_NUMBER_FIELD)))
                .withColumn(WEEK_NUMBER_FIELD, row_number().over(weekWindow))
                .withColumn(ROLLING_AVG_VALUE_FIELD, avg(TOTAL_VALUE_FIELD).over(weekWindow.rowsBetween(-2, 1)))
                .withColumn(CENTRED_AVG_VALUE_FIELD, avg(ROLLING_AVG_VALUE_FIELD).over(weekWindow.rowsBetween(0, 1)))
                .withColumn(XY_VALUE_FIELD, col(CENTRED_AVG_VALUE_FIELD).multiply(col(WEEK_NUMBER_FIELD)))
                .withColumn(X_POW_2_FIELD, pow(col(WEEK_NUMBER_FIELD), 2))
                .withColumn(YEAR_WEEK_NUMBER, weekofyear(col(WEEK_DATE_FIELD)));

        // weeks info for aggregations and statistics
        long weeksCount = this.weekData.count();

        log.trace("DataProcessing job: Loaded ORDER data for {} weeks", weeksCount);

        Dataset<Row> weekAggregationData = this.weekData
                .agg(
                        sum(XY_VALUE_FIELD).as(XY_SUM_FIELD),
                        sum(WEEK_NUMBER_FIELD).as(X_SUM_FIELD),
                        sum(CENTRED_AVG_VALUE_FIELD).as(Y_SUM_FIELD),
                        sum(X_POW_2_FIELD).as(X_POW_2_SUM_FIELD),
                        countDistinct(CENTRED_AVG_VALUE_FIELD).as(CENTRED_AVG_VALUE_FIELD))
                .withColumn(M_VALUE_FIELD, col(CENTRED_AVG_VALUE_FIELD).multiply(col(XY_SUM_FIELD)).minus(col(X_SUM_FIELD).multiply(col(Y_SUM_FIELD)))
                        .divide(col(CENTRED_AVG_VALUE_FIELD).multiply(col(X_POW_2_SUM_FIELD)).minus(pow(col(X_SUM_FIELD), 2))))
                .withColumn(B_VALUE_FIELD, col(Y_SUM_FIELD).minus(col(M_VALUE_FIELD).multiply(col(X_SUM_FIELD))).divide(col(CENTRED_AVG_VALUE_FIELD)));

        SimpleRowWrapper weeksInfo = new SimpleRowWrapper(weekAggregationData.collectAsList().get(0));
        log.trace("DataProcessing job : Loaded weeks info");

        double m = weeksInfo.getDouble(M_VALUE_FIELD, 0.0);
        log.trace("DataProcessing job : m value is - {}", m);

        double b = weeksInfo.getDouble(B_VALUE_FIELD, 0.0);
        log.trace("DataProcessing job : b value is - {}", m);

        WeekFields weekFields = WeekFields.of(DayOfWeek.MONDAY, 3);

        this.weekData = this.weekData.withColumn(WEEK_TREND_VALUE, lit(m).multiply(col(WEEK_NUMBER_FIELD)).plus(lit(b)))
                .withColumn(ACTUAL_DIFF_VALUE, col(TOTAL_VALUE_FIELD).minus(col(WEEK_TREND_VALUE)))
                .withColumn(ACTUAL_MULTIPLY_VALUE, col(TOTAL_VALUE_FIELD).divide(col(WEEK_TREND_VALUE)));

        // pairs week_no -> diff to the prediction value
        Map<Integer, Double> weekSeasons = this.weekData.groupBy(YEAR_WEEK_NUMBER)
                .agg(coalesce(avg(ACTUAL_DIFF_VALUE), lit(0.0)).as(WEEK_DIFF_AVG))
                .collectAsList()
                .stream().collect(HashMap::new, (map, row) -> {
                    SimpleRowWrapper rowWrapper = new SimpleRowWrapper(row);
                    map.put(rowWrapper.getInt(YEAR_WEEK_NUMBER), rowWrapper.getDouble(WEEK_DIFF_AVG, 0.0));
                }, HashMap::putAll);

        log.trace("DataProcessing job : Loaded week seasons info");

        WindowSpec weekSeasonalityWindow = Window.partitionBy(YEAR_WEEK_NUMBER);

        this.weekData = this.weekData
                .withColumn(WEEK_SEASONALITY_VALUE_FIELD, coalesce(avg(ACTUAL_DIFF_VALUE).over(weekSeasonalityWindow), lit(0.0)))
                .withColumn(FORECAST_FIELD,
                        lit(col(WEEK_SEASONALITY_VALUE_FIELD).plus(col(WEEK_TREND_VALUE))));

        log.trace("DataProcessing job : Constructed week data");

        double standardDeviation = new SimpleRowWrapper(this.weekData.select(stddev(col(WEEK_TREND_VALUE).minus(col(TOTAL_VALUE_FIELD))).as(STD_DEV_FIELD))
                .collectAsList().get(0)).getDouble(STD_DEV_FIELD, 0.0);

        log.trace("DataProcessing job : Calculated standard deviation: {}", standardDeviation);

        // calculated predictions for the next 'WEEKS_FOR_FORECAST' (52) weeks
        this.predictions = LongStream.iterate(1, l -> ++l)
                .limit(WEEKS_FOR_FORECAST)
                .mapToObj(h -> {
                    // ternary operator to eliminate duplicate week in year data for FridayJob
                    LocalDateTime dateTime = currentDay
                            .plusWeeks(h - 1);
                    Double trendDiff = weekSeasons.getOrDefault(dateTime.get(weekFields.weekOfWeekBasedYear()), 1.0);

                    double deviation = 0;
                    if(!Double.valueOf(standardDeviation).isNaN()){
                        deviation = C_CONSTANT_FOR_PREDICTION_INTERVALS * standardDeviation * Math.sqrt(h * (1 + (double) h / (weeksCount)));
                    }

                    BigDecimal forecast = toBigDecimal(Math.max(trendDiff + (m * (weeksCount + h) + b), 0D));

                    return new YearChartPoint(asDate(dateTime),
                            forecast.setScale(2, RoundingMode.HALF_UP),
                            BigDecimal.valueOf(Math.max(forecast.doubleValue() - deviation, 0.0)).setScale(2, RoundingMode.HALF_UP),
                            forecast.add(BigDecimal.valueOf(deviation).setScale(2, RoundingMode.HALF_UP))
                    );
                }).collect(Collectors.toList());

        log.trace("DataProcessing job : Loaded predictions info");


        // existing data points
        this.existingData = this.weekData.orderBy(col(WEEK_NUMBER_FIELD).desc())
                .collectAsList()
                .stream()
                .map(SimpleRowWrapper::new)
                .map(wr -> new YearChartPoint(asDate(wr.getTimestamp(WEEK_DATE_FIELD).toLocalDateTime()),
                        BigDecimal.valueOf(wr.getDouble(TOTAL_VALUE_FIELD, 0.0)).setScale(2, RoundingMode.HALF_UP)))
                .collect(Collectors.toList());

        log.trace("DataProcessing job : Loaded existing data info");

        if (!existingData.isEmpty()) {
            log.trace("DataProcessing job : Actual value for last week is {}", existingData.get(existingData.size() - 1).getForecast());
        } else {
            log.trace("DataProcessing job : Existing data is empty");
        }

        log.trace("DataProcessing job : Prediction value for current week is {}", predictions.get(0).getForecast());

        // final year chart data
        this.yearData = Stream.concat(this.existingData.stream().limit(GRAPH_WEEKS_COUNT / 2),
                        this.predictions.stream())
                .sorted(Comparator.comparing(YearChartPoint::getDate))
                .limit(GRAPH_WEEKS_COUNT)
                .collect(Collectors.toList());

        log.trace("DataProcessing job : Loaded year data info");

        this.last20weeksInfo = this.existingData.subList(0, Math.min(this.existingData.size(), 20))
                .stream().map(YearChartPoint::getActual).collect(Collectors.toList());

        log.trace("DataProcessing job : Loaded info for last 20 weeks");

        // final year chart data
        List<YearChartPoint> allGeneratedData = Stream.concat(this.existingData.stream(),
                        this.predictions.stream())
                .sorted(Comparator.comparing(YearChartPoint::getDate).reversed())
                .collect(Collectors.toList());

        this.quarterData = IntStream.range(1, 5)
                .mapToObj(i -> {
                    YearQuarterHolder yearQuarter = new YearQuarterHolder(setYearStartRelatively(configuredYearStart, currentDay.toLocalDate()),
                            i, asDate(currentDay));

                    return new QuarterData(allGeneratedData.stream()
                            .filter(w -> yearQuarter.isInQ(w.getDate()))
                            .map(y -> Optional.ofNullable(y.getForecast()).orElseGet(y::getActual))
                            .reduce(BigDecimal.ZERO, BigDecimal::add), yearQuarter.getCalendarQ(), yearQuarter.getStatus());
                })
                .collect(Collectors.toList());

        if (log.isTraceEnabled()) {
            log.trace("DataProcessing job: Loaded quarter data:");
            this.quarterData.forEach(q -> log.trace("DataProcessing job: Value for {} quarter: {}", q.getName(), q.getForecastValue()));
        }
    }

    public BigDecimal getYearForecast(){
        return this.quarterData.stream()
                .map(QuarterData::getForecastValue)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    public ForecastCalculationHolder(Dataset<Row> ordersDataSet, LocalDateTime currentDay,
                                     LocalDate configuredYearStart) {
        this(ordersDataSet, currentDay, configuredYearStart, BigDecimal.ZERO);
    }

}
