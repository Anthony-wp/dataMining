package dpl.processing.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum QuarterStatus {
    ACTUAL("Actual"),
    FORECAST("Forecast");

    private final String name;
}
