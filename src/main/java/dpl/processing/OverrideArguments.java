package dpl.processing;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import lombok.Getter;
import lombok.ToString;

@Parameters(separators = " ")
@ToString
@Getter
public class OverrideArguments {
    @Parameter(names = {"-h", "--help"}, description = "Whether to show help info")
    private boolean helpMode = false;

    @Parameter(names = {"-data", "--default-data-limit"}, description = "Default amount of data to process")
    private int defaultDataLimit = Integer.MAX_VALUE;

    @Parameter(names = {"-tc", "--tenant-codes"}, description = "Comma separated list of tenant codes to restrict the run to")
    private String tenantCodes;

    @Parameter(names = {"-sm", "--spark-memory"}, description = "The amount of memory for the spark process")
    private String sparkMemory;

    @Parameter(names = {"-scpe", "--spark-cores-per-executor"}, description = "The number of cores per executor for spark")
    private Integer sparkCoresPerExecutor;

    @Parameter(names = {"-sei", "--spark-executor-instances"}, description = "The number of instances per executor in spark")
    private Integer sparkExecutorInstances;

    @Parameter(names = {"-smc", "--spark-max-cores"}, description = "The max number of cores to use")
    private Integer sparkMaxCores;

    @Parameter(names = {"-sbt", "--spark-broadcast-timeout"}, description = "The number of seconds to allow for broadcast join")
    private Integer sparkBroadCastTimeout = 0;

    @Parameter(names = {"-sab", "--spark-auto-broadcast"}, description = "Whether to allow Spark broadcast join", arity = 1)
    private boolean sparkAutoBroadCast = true;

    @Parameter(names = {"-spark", "--spark-master"}, description = "The spark master to use")
    private String sparkMaster;

    @Parameter(names = {"-shm", "--shopify-monday"}, description = "Force run shopify monday card job for selected orgs")
    private boolean forceRunShopifyMondayJob = false;

    @Parameter(names = {"-shtu", "--shopify-tuesday"}, description = "Force run shopify tuesday card job for selected orgs")
    private boolean forceRunShopifyTuesdayJob = false;

    @Parameter(names = {"-shw", "--shopify-wednesday"}, description = "Force run shopify wednesday card job for selected orgs")
    private boolean forceRunShopifyWednesdayJob = false;

    @Parameter(names = {"-shth", "--shopify-thursday"}, description = "Force run shopify thursday card job for selected orgs")
    private boolean forceRunShopifyThursdayJob = false;

    @Parameter(names = {"-shf", "--shopify-friday"}, description = "Force run shopify friday card job for selected orgs")
    private boolean forceRunShopifyFridayJob = false;

    @Parameter(names = {"-shst", "--shopify-saturday"}, description = "Force run shopify saturday card job for selected orgs")
    private boolean forceRunShopifySaturdayJob = false;

    @Parameter(names = {"-shs", "--shopify-sunday"}, description = "Force run shopify sunday card job for selected orgs")
    private boolean forceRunShopifySundayJob = false;

    @Parameter(names = {"-sht", "--shopify-timestamp"}, description = "Specify timestamp (yyyy-MM-dd'T'HH:mm:ss.SSS) for shopify card jobs for selected orgs")
    private String shopifyJobTimestamp = null;

    @Parameter(names = {"-rec", "--reschedule-empty-card"})
    private boolean rescheduleEmptyCard = false;

    @Parameter(names = {"-sp", "--save-path"})
    private String savePath = "/mnt/";

    @Parameter(names = {"-save", "--save-spark-data-stages"})
    private boolean outputSparkStagesToFile = false;

    public boolean isHelpMode() {
        return helpMode;
    }
}
