package dpl.processing.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class DataPipelineScheduleService {

    @Autowired
    private JobSchedulerService jobSchedulerService;

    public void runService() {
        jobSchedulerService.scheduleDataProcessingJob();
    }

}
