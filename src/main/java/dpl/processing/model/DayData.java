package dpl.processing.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Optional;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DayData implements Serializable {

    private BigDecimal forecast;

    private BigDecimal actual;

    public DayData(BigDecimal forecast) {
        this.forecast = forecast;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public BigDecimal getDelta() {
        if (forecast == null || actual == null) {
            return null;
        }
        return actual.subtract(forecast);
    }

    @JsonIgnore
    public static final DayData EMPTY_FORECAST = new DayData(BigDecimal.ZERO);

    @JsonIgnore
    public BigDecimal getActualOtherwiseForecast(){
        return Optional.ofNullable(actual).orElse(forecast);
    }
}
