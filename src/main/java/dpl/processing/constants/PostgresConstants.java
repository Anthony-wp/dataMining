package dpl.processing.constants;

import lombok.experimental.UtilityClass;

@UtilityClass
public class PostgresConstants {
    public static final String SHOP_GLOBAL_KEYSPACE = "testshop";
    public static final String KEYSPACE = "keyspace";
    public static final String TABLE = "dbtable";

    public static final String ID_FIELD = "id";
    public static final String CUSTOMER_ID_FIELD = "customer_id";

    public static final String ENGAGEMENT_GAP_SECONDS_FIELD = "browse_gap_seconds";
}
