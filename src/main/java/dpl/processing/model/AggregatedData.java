package dpl.processing.model;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import lombok.*;

import javax.persistence.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor

@Entity
@Inheritance
@Table(schema = "testshop", name = "aggregated_data")
@Builder
public class AggregatedData {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", updatable = false, nullable = false)
    private Long id;

    @Column(name = "created_date")
    private Date createdDate;

    protected AggregatedData(Long id, Date createdDate, Date cardDate) {
        this.id = id;
        this.createdDate = createdDate;
    }

    @Getter
    private BigDecimal totalSales;

    @Getter
    private BigDecimal yesterdaySales;

    @Getter
    private BigDecimal yesterdayAverageOrderValue;

    @Getter
    private BigDecimal last7daysAverageSales;

    @Getter
    private BigDecimal last7daysAverageOrderValue;

    @Getter
    private Long last30daysActiveCustomers;

    @Getter
    private Long last30daysLostCustomers;

    @Getter
    private Long riskOfLeavingCustomers;

    private Long loyalRiskOfLeavingCustomers;

    private Integer last20weeksRanking;

    @ElementCollection
    @CollectionTable(name = "last_20_weeks_values", joinColumns = @JoinColumn(name = "aggregated_data_id"))
    @Column(name = "value")
    private List<BigDecimal> last20weeksValues;

    private WeekData weekData;

    private Long lastWeekOrders;

    private Long lastWeekCustomers;

    private Long lastWeekNewCustomers;

    private BigDecimal lastWeekAverageOrderValue;

    private BigDecimal salesLastWeek;

    private BigDecimal salesLastYearWeek;

    private BigDecimal forecastLastYearWeek;

    private QuarterData quarter0;

    private QuarterData quarter1;

    private QuarterData quarter2;

    private QuarterData quarter3;

    private YearData yearData;

    private BigDecimal yearForecast;

    private BigDecimal lastYearAverageOrderValue;

    private Long activeCustomers;

    private Long atRiskCustomers;

    private BigDecimal activeCustomersLastYearSpend;

    private BigDecimal atRiskCustomersLastYearSpend;

    private SegmentData highValueSegmentData;

    private SegmentData averageValueSegmentData;

    private SegmentData lowValueSegmentData;

    public Long getLastWeekReturningCustomers() {
        if (lastWeekCustomers == null || lastWeekNewCustomers == null) {
            return null;
        }
        return lastWeekCustomers - lastWeekNewCustomers;
    }

    public BigDecimal getLastWeekNewCustomersPercent() {
        return Optional.ofNullable(lastWeekCustomers)
                .filter(lwc -> lwc != 0)
                .map(lwc -> BigDecimal.valueOf(lastWeekNewCustomers * 100.0 / lwc))
                .orElse(BigDecimal.ZERO);
    }

    public BigDecimal getLastYearWeekSalesGrowsPercents() {
        return Optional.ofNullable(salesLastYearWeek)
                .filter(s -> BigDecimal.ZERO.compareTo(s) != 0)
                .map(s -> (weekData.getWeekForecast().subtract(s)).divide(s, 2, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100.0)))
                .orElse(null);
    }

    public BigDecimal getLastYearWeekForecastGrowsPercents() {
        return Optional.ofNullable(forecastLastYearWeek)
                .filter(s -> BigDecimal.ZERO.compareTo(s) != 0)
                .map(s -> weekData.getWeekForecast().multiply(BigDecimal.valueOf(100.0)).divide(s, 2, RoundingMode.HALF_UP))
                .orElse(null);
    }

    public BigDecimal getLastWeekReturningCustomersPercent() {
        return BigDecimal.valueOf(100.0).subtract(getLastWeekNewCustomersPercent());
    }


    public Integer getForecastWeekPosition() {
        if (weekData == null) {
            return null;
        }
        ArrayList<BigDecimal> weeks = new ArrayList<>(last20weeksValues);
        weeks.add(weekData.getWeekForecast());
        weeks.sort(Collections.reverseOrder());
        return Math.min(weeks.indexOf(weekData.getWeekForecast()) + 1, 20);
    }

    public BigDecimal getLoyalRiskOfLeavingCustomersPercentage() {
        if (loyalRiskOfLeavingCustomers == null || riskOfLeavingCustomers == null || riskOfLeavingCustomers == 0L) {
            return null;
        }

        return BigDecimal.valueOf(loyalRiskOfLeavingCustomers * 100.0 / riskOfLeavingCustomers)
                .setScale(0, RoundingMode.HALF_UP);
    }
}
