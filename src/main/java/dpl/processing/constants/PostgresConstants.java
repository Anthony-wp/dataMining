package dpl.processing.constants;

import lombok.experimental.UtilityClass;

@UtilityClass
public class PostgresConstants {
    public static final String SHOP_GLOBAL_KEYSPACE = "testshop";
    public static final String DISTIL_ORG_KEYSPACE_PREFIX = "distil_org_";
    public static final String KEYSPACE = "keyspace";
    public static final String TABLE = "dbtable";

    public static final String TENANT_CODE_FIELD = "tc";
    public static final String CUSTOMER_KEY_FIELD = "customer_key";
    public static final String CUSTOMER_EXTERNAL_ID_FIELD = "external_id";

    public static final String ENGAGEMENT_SESSION_ID_FIELD = "session_id";
    public static final String ENGAGEMENT_ID_FIELD = "engagement_id";
    public static final String ENGAGEMENT_DAY_TIME_KEY_FIELD = "day_timekey";
    public static final String ENGAGEMENT_CUSTOMER_ID_FIELD = "customer_customerid";
    public static final String ENGAGEMENT_EVENT_TIMESTAMP_FIELD = "context_eventtimestamputc";
    public static final String ENGAGEMENT_GAP_SECONDS_FIELD = "browse_gap_seconds";
    public static final String ENGAGEMENT_ORDER_ID_FIELD = "order_orderid";
    public static final String ENGAGEMENT_ANONYMOUS_USER_ID_FIELD = "context_anonymoususerid";

    public static String buildOrgKeyspace(String tenant) {
        return DISTIL_ORG_KEYSPACE_PREFIX + tenant;
    }
}
