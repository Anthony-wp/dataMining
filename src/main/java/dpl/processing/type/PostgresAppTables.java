package dpl.processing.type;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public enum PostgresAppTables {
    EXTERNAL_ID_ORDER_KEY_TABLE("external_id_order_key"),
    EXTERNAL_ID_CONTENT_KEY_TABLE("external_id_content_key"),
    EXTERNAL_ID_PRODUCT_KEY_TABLE("external_id_product_key"),
    EXTERNAL_ID_CUSTOMER_KEY_TABLE("external_id_customer_key"),
    ANONYMOUS_ID_CUSTOMER_KEY_TABLE("anonymous_id_customer_key"),
    EMAIL_ADDRESS_CUSTOMER_KEY_TABLE("email_address_customer_key"),
    TELEPHONE_NUM_CUSTOMER_KEY_TABLE("telephone_num_customer_key"),
    DS_OBJECT_HASH_TABLE("ds_object_hash"),
    DS_UPDATE_PAYLOADS_TABLE("ds_update_payloads"),
    ENRICHMENT_CHRONOTYPE_TABLE("enrichment_chronotype"),
    ENRICHMENT_SPEND_BRACKET_TABLE("enrichment_spend_bracket"),
    CONTENT_TABLE("content"),
    PRODUCT_TABLE("product"),
    TRACKING_TABLE("tracking"),
    ENGAGEMENT_TABLE("engagement"),
    PURCHASE_HISTORY_TABLE("purchase_history"),
    ENGAGEMENT_BY_DAY_TABLE("engagement_by_day"),
    PRODUCT_VIEW_MATRIX_TABLE("product_view_matrix"),
    PRODUCT_TRENDING_SET_TABLE("product_trending_set"),
    PRODUCT_PURCHASED_MATRIX_TABLE("product_purchased_matrix"),
    PRODUCT_VIEWED_HISTORY_SET_TABLE("product_viewed_history_set"),
    PRODUCT_FOLLOW_ON_PURCHASE_MATRIX_TABLE("product_follow_on_purchase_matrix");

    private String tableName;

    PostgresAppTables(String tableName) {
        this.tableName = tableName;
    }
}
