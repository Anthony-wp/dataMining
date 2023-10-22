package dpl.processing.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;

import java.util.function.Function;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

@Getter
@RequiredArgsConstructor
public enum SegmentType {
    HIGH_VALUE(rankColumn -> col(rankColumn).$greater(lit(0.75)), AggregatedData::getHighValueSegmentData),
    AVERAGE_VALUE(rankColumn -> col(rankColumn).$greater$eq(lit(0.25)).and(col(rankColumn).$less$eq(lit(0.75))),
            AggregatedData::getAverageValueSegmentData),
    LOW_VALUE(rankColumn -> col(rankColumn).lt(lit(0.25)), AggregatedData::getLowValueSegmentData);

    private final Function<String, Column> buildFilterColumn;
    private final Function<AggregatedData, SegmentData> getSegmentData;
}
