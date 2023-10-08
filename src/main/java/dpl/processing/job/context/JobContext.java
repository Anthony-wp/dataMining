package dpl.processing.job.context;

import java.time.LocalDateTime;

public interface JobContext {

    LocalDateTime getJobEntryTimestamp();

    boolean isValid();
}
