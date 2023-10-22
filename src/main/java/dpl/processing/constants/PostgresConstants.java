package dpl.processing.constants;

import lombok.experimental.UtilityClass;

@UtilityClass
public class PostgresConstants {
    public static final String SHOP_GLOBAL_KEYSPACE = "testshop";
    public static final String KEYSPACE = "keyspace";
    public static final String TABLE = "dbtable";

    public static final String ID_FIELD = "id";
    public static final String ORDER_CUSTOMER_ID_FIELD = "customer_id";
    public static final String USER_ID_FIELD = "user_id";

    public static final String ENGAGEMENT_GAP_SECONDS_FIELD = "browse_gap_seconds";

    public static final String PRODUCT_ID_FIELD = "product_id";
    public static final String ORDER_PRODUCT_ID_FIELD = "product_ex_id";
    public static final String ORDER_ID_FIELD = "order_id";
    public static final String ORDER_CUSTOMER_EMAIL_ADDRESS = "customer_email_address";
    public static final String ORDER_TIMESTAMP = "timestamp";
    public static final String ORDER_LINE_ITEM_QTY = "qty";
    public static final String LINE_VALUE_EXCLUDING_TAX = "line_value_excluding_tax";
    public static final String LINE_VALUE_INCLUDING_TAX = "line_value_including_tax";
    public static final String ORDER_PAYMENT = "payment_method";
    public static final String ORDER_BILLING_ADDRESS_LINE1 = "billing_address_line1";
    public static final String ORDER_BILLING_ADDRESS_LINE2 = "billing_address_line2";
    public static final String ORDER_BILLING_ADDRESS_LINE3 = "billing_address_line3";
    public static final String ORDER_BILLING_ADDRESS_POSTCODE = "billing_address_line_postal_code";
    public static final String ORDER_BILLING_ADDRESS_REGION = "billing_address_line_region";
    public static final String ORDER_BILLING_ADDRESS_TOWN = "billing_address_line_town";
    public static final String ORDER_BILLING_ADDRESS_COUNTRY = "billing_address_country";
    public static final String ORDER_DELIVERY_ADDRESS_LINE1 = "postal_address_line1";
    public static final String ORDER_DELIVERY_ADDRESS_LINE2 = "postal_address_line2";
    public static final String ORDER_DELIVERY_ADDRESS_LINE3 = "postal_address_line3";
    public static final String ORDER_DELIVERY_ADDRESS_POSTCODE = "postal_address_postal_code";
    public static final String ORDER_DELIVERY_ADDRESS_REGION = "postal_address_region";
    public static final String ORDER_DELIVERY_ADDRESS_TOWN = "postal_address_line_town";
    public static final String ORDER_DELIVERY_ADDRESS_COUNTRY = "postal_address_country";
    public static final String ORDER_CURRENCY = "currency";
    public static final String ORDER_DISCOUNT_CODE = "discount_code";
    public static final String ORDER_DISCOUNT_AMOUNT = "discount_amount";
    public static final String ORDER_PAYMENT_STATUS = "status";

    public static final String PRODUCT_NAME = "name";
    public static final String PRODUCT_SHOP_URL = "url";
    public static final String PRODUCT_IMAGE_URL = "image";
    public static final String PRODUCT_AVAILABLE = "available";
    public static final String PRODUCT_STOCK_UNITS = "stock";
    public static final String PRODUCT_CATEGORY = "category";

//    public static final String PRODUCT_LIST_PRICE_EX_TAX = "product_price_ex_tax";
//    public static final String PRODUCT_LIST_PRICE_INC_TAX = "product_price_inc_tax";
//    public static final String PRODUCT_PRICE_BREAKS_DESCRIPTION = "product_price_breaks_description";

//    public static final String CONTENT_EXTERNAL_ID = "content_id";
//    public static final String CONTENT_TITLE = "content_title";
//    public static final String CONTENT_URL = "content_url";
//    public static final String CONTENT_IMAGE_URL = "content_image_url";
//    public static final String CONTENT_PRECIS = "content_precis";
//    public static final String CONTENT_AVAILABLE = "content_available";
//    public static final String CONTENT_KEYWORD = "content_keyword";
//    public static final String CONTENT_PUBLISHED = "content_date_published";
}
