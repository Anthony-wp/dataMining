package dpl.processing.model;


import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;

@Data
@Builder(toBuilder=true)
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CardHeader implements Serializable {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date date;


    private BigDecimal totalSales;

    private BigDecimal yesterdaySales;

    private BigDecimal yesterdayAverageOrderValue;

    private BigDecimal last7daysAverageSales;

    private BigDecimal last7daysAverageOrderValue;

    private Long last30daysActiveCustomers;

    private Long last30daysLostCustomers;

    private Long riskOfLeavingCustomers;

    private Long loyalRiskOfLeavingCustomers;

    @JsonIgnore
    private String locale;
    @JsonIgnore
    private String currency;

    public BigDecimal getLoyalRiskOfLeavingCustomersPercentage() {
        if (loyalRiskOfLeavingCustomers == null || riskOfLeavingCustomers == null || riskOfLeavingCustomers == 0L) {
            return null;
        }

        return BigDecimal.valueOf(loyalRiskOfLeavingCustomers * 100.0 / riskOfLeavingCustomers)
                .setScale(0, RoundingMode.HALF_UP);
    }
}
