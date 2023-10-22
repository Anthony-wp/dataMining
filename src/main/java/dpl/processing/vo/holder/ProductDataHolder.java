package dpl.processing.vo.holder;

import lombok.Data;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

@Data
public class ProductDataHolder {
    private String id;
    private String name;
    private Long count;
    private BigDecimal totalValue;
    private Set<String> customerKeys;

    public ProductDataHolder(String id, String name) {
        this.id = id;
        this.name = name;
        this.count = 0L;
        this.totalValue = BigDecimal.ZERO;
        this.customerKeys = new HashSet<>();
    }

    public void incrementCount() {
        count++;
    }

    public void addValue(double value) {
        totalValue = totalValue.add(BigDecimal.valueOf(value));
    }

    public BigDecimal getTotalValue() {
        return totalValue.setScale(2, BigDecimal.ROUND_HALF_UP);
    }

    public void addCustomerKey(String customerKey) {
        if (customerKey != null) {
            customerKeys.add(customerKey);
        }
    }

    public int getCustomerCount() {
        return customerKeys.size();
    }

    public BigDecimal customerPercentFromTotal(long total) {
        return BigDecimal.valueOf(getCustomerCount())
                .movePointRight(2)
                .divide(BigDecimal.valueOf(total), 2, BigDecimal.ROUND_HALF_UP);
    }
}
