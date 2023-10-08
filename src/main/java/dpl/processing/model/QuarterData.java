package dpl.processing.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class QuarterData implements Serializable {

    private BigDecimal forecastValue;

    private String name;
    private QuarterStatus status;
}
