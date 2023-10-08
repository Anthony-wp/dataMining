package dpl.processing.service;

import dpl.processing.job.DataProcessingJob;
import dpl.processing.job.context.ProcessJobContext;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
@Slf4j
public class JobSchedulerService {

    @Autowired
    private DataProcessingJob dataProcessingJob;

    public void scheduleDataProcessingJob() {
        dataProcessingJob.startJob();
    }
}
