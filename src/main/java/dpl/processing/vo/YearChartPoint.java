package dpl.processing.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class YearChartPoint implements Serializable {

    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date date;

    private BigDecimal actual;

    private BigDecimal forecast;

    private BigDecimal lowerBound;

    private BigDecimal higherBound;

    public YearChartPoint(Date date, BigDecimal actual) {
        this.date = date;
        this.actual = actual;
    }

    @Builder
    public YearChartPoint(Date date, BigDecimal forecast, BigDecimal lowerBound, BigDecimal higherBound) {
        this.date = date;
        this.forecast = forecast;
        this.lowerBound = lowerBound;
        this.higherBound = higherBound;
    }
}
