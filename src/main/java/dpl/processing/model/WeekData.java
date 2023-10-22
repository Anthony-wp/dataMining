package dpl.processing.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class WeekData implements Serializable {

    private DayData monday;

    private DayData tuesday;

    private DayData wednesday;

    private DayData thursday;

    private DayData friday;

    private DayData saturday;

    private DayData sunday;

    public BigDecimal getWeekForecast() {
        return Stream.of(monday, tuesday, wednesday, thursday, friday, saturday, sunday)
                .filter(Objects::nonNull)
                .map(DayData::getForecast)
                .filter(Objects::nonNull)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    public BigDecimal getWeekActualValue() {
        return Stream.of(monday, tuesday, wednesday, thursday, friday, saturday, sunday)
                .filter(Objects::nonNull)
                .map(DayData::getActual)
                .filter(Objects::nonNull)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    @JsonIgnore
    public List<DayData> getDaysList() {
        return Stream.of(monday, tuesday, wednesday, thursday, friday, saturday, sunday)
                .collect(Collectors.toList());
    }


    public BigDecimal getWeekForecastForFridayCard(){
        return getDaysList().stream()
                .map(DayData::getActualOtherwiseForecast)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}
