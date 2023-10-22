package dpl.processing.model;

import dpl.processing.vo.YearChartPoint;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class YearData implements Serializable {

    private List<YearChartPoint> chartData;
}
