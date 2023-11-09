package dpl.processing.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductSoldInfo implements Serializable {

    private String name;

    private BigDecimal totalValue;

    private BigDecimal lastWeekValue;

    private Integer lastWeekPosition;

    private Integer weekBeforeLastPosition;

    public BigDecimal getLastWeekValuePercent() {
        return lastWeekValue;
    }

    public String getRankingChange() {
        if (weekBeforeLastPosition == null || weekBeforeLastPosition == 0) {
            return "New";
        }

        int diff = weekBeforeLastPosition - lastWeekPosition;
        return diff > 0 ? "+" + diff : Integer.toString(diff);
    }

    public Boolean isEmpty(){
        return this.getName() == null || this.getTotalValue() == null;
    }
}
