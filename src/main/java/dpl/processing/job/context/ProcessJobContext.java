package dpl.processing.job.context;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.LocalDateTime;
import java.time.YearMonth;

@Getter
@AllArgsConstructor
public class ProcessJobContext implements JobContext{
//    private final Long connectionId;
    private final Boolean forceRun;
    private LocalDateTime jobEntryTimestamp;

    @Override
    public boolean isValid() {
        return jobEntryTimestamp != null;
    }
}
