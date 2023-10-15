package dpl.processing.service;

import dpl.processing.job.DataProcessingJob;
import dpl.processing.job.context.ProcessJobContext;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@AllArgsConstructor
@Slf4j
public class JobSchedulerService {

    @Autowired
    private DataProcessingJob dataProcessingJob;

    @Scheduled(fixedRate=60 * 60 * 1000)
    public void scheduleDataProcessingJob() {
        dataProcessingJob.startJob(new ProcessJobContext(true, LocalDateTime.now()));
    }
}
