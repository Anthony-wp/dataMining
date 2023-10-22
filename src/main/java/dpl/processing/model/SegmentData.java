package dpl.processing.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentData implements Serializable {

    private Long numberOfCustomers;

    private BigDecimal averageSpend;

    private Long allActiveCustomers;

    private Long activeCustomers;

    private BigDecimal totalRevenueCustomers;

    private BigDecimal revenueCustomers;

    private Long oneOrderCustomers;

    private Long totalOrderNumber;

    private BigDecimal averageOrderValue;

    private BigDecimal averageVisitFrequencySeconds;

    private BigDecimal averagePurchaseFrequency;

    private List<ProductData> mostPopularProducts;

    private ProductData leadingPurchase;

    private List<ChartPoint> chartData;

    public BigDecimal getActiveCustomersPercentage() {
        return allActiveCustomers == null || allActiveCustomers == 0 || activeCustomers == null || activeCustomers == 0 ?
                BigDecimal.ZERO :
                round(BigDecimal.valueOf(activeCustomers * 100.0 / allActiveCustomers));
    }

    public BigDecimal getOneOrderCustomersPercentage() {
        return numberOfCustomers == null || numberOfCustomers == 0 ?
                BigDecimal.ZERO :
                round(BigDecimal.valueOf(oneOrderCustomers * 100.0 / numberOfCustomers));
    }

    public BigDecimal getMultipleOrderCustomersPercentage() {
        return round(BigDecimal.valueOf(100.0).subtract(getOneOrderCustomersPercentage()));
    }

    public BigDecimal getLast12MonthsActiveCustomersSalesPercentage() {
        return totalRevenueCustomers == null || BigDecimal.ZERO.compareTo(totalRevenueCustomers) == 0
                || revenueCustomers == null || BigDecimal.ZERO.compareTo(revenueCustomers) == 0  ?
                BigDecimal.ZERO :
                round(revenueCustomers.divide(totalRevenueCustomers, 2, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100D)));
    }

    public String getAverageVisitFrequencyFormatted(){
        if(averagePurchaseFrequency == null) {
            return "";
        }
        return getFormattedFrequency(averageVisitFrequencySeconds.longValue());
    }

    public String getAveragePurchaseFrequencyFormatted(){
        if(averagePurchaseFrequency == null) {
            return "";
        }
        return getFormattedFrequency(averagePurchaseFrequency.longValue());
    }

    @JsonIgnore
    private String getFormattedFrequency(Long frequency) {
        if (frequency == null || frequency <= 0) {
            return "";
        } else if (frequency < 60) {
            return String.format("%d seconds", frequency);
        } else if (frequency < 3600) {
            return String.format("%d minutes and %d seconds", frequency / 60, frequency % 60);
        } else if (frequency < 86400) {
            return String.format("%d hours and %d minutes", frequency / 3600, frequency % 3600 / 60);
        } else {
            return String.format("%d days and %d hours", frequency / 86400, frequency % 86400 / 3600);
        }
    }

    @JsonIgnore
    protected BigDecimal round(BigDecimal num) {
        return num.setScale(2, RoundingMode.HALF_UP);
    }
}
