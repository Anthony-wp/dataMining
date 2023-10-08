package dpl.processing;

import com.beust.jcommander.JCommander;
import dpl.processing.service.DataPipelineScheduleService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@Slf4j
@SpringBootApplication
@ComponentScan({"dpl.processing.service", "dpl.processing.job", "dpl.processing.config"})
public class DataProcessingService {
    private static final OverrideArguments overrideArguments = new OverrideArguments();

    public static OverrideArguments getOverrideArguments() {
        return overrideArguments;
    }

    public static void main(String[] args) {
        startApp(args).getBean(DataPipelineScheduleService.class).runService();
    }

    private static ConfigurableApplicationContext startApp(String[] args) {
        try {
            fillArguments(args);

            return new SpringApplicationBuilder()
                    .sources(DataProcessingService.class)
                    .bannerMode(Banner.Mode.OFF)
                    .web(WebApplicationType.SERVLET)
                    .run(args);
        } catch (Exception e) {
            log.error("Can't start app.", e);
            throw new RuntimeException(e);
        }

    }

    private static void fillArguments(String[] args) {
        try {
            JCommander commander = new JCommander(overrideArguments, args);

            if (overrideArguments.isHelpMode()) {
                commander.usage();
                System.exit(0);
            }
        } catch (Exception e) {
            log.error("Error trying to parse command line args: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

}
