package dpl.processing.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChartPoint implements Serializable {

    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date date;

    private BigDecimal value;
}
