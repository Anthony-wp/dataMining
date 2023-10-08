package dpl.processing.model;

import lombok.*;

import javax.persistence.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor

@Entity
@Inheritance
@Table(schema = "testshop", name = "aggregated_data")
@Builder
public class AggregatedData {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "shopify_card_generator")
    @SequenceGenerator(name = "shopify_card_generator", sequenceName = "distil_org_xx.shopify_card_seq", allocationSize = 1)
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

    public BigDecimal getLoyalRiskOfLeavingCustomersPercentage() {
        if (loyalRiskOfLeavingCustomers == null || riskOfLeavingCustomers == null || riskOfLeavingCustomers == 0L) {
            return null;
        }

        return BigDecimal.valueOf(loyalRiskOfLeavingCustomers * 100.0 / riskOfLeavingCustomers)
                .setScale(0, RoundingMode.HALF_UP);
    }

}
