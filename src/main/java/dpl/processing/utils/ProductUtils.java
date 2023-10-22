package dpl.processing.utils;

import dpl.processing.model.ProductData;
import dpl.processing.vo.holder.ProductDataHolder;
import dpl.processing.vo.wrapper.row.SimpleRowWrapper;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.Seq;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static dpl.processing.constants.PostgresConstants.*;
import static dpl.processing.utils.ScalaUtils.toSeq;
import static org.apache.spark.sql.functions.*;

@UtilityClass
public class ProductUtils {

    private static final String STRUCT_PRODUCT_ID_FIELD = "struct_product_id";
    private static final String STRUCT_PRODUCT_NAME_FIELD = "struct_product_name";
    private static final String STRUCT_PRODUCT_PRICE_FIELD = "struct_product_price";
    private static final Seq<String> USER_ID_TO_JOIN = toSeq(Collections.singletonList(USER_ID_FIELD));
    private static final String TOTAL_VALUE_FIELD = "total_value";
    private static final String PRODUCT_IDS = "product_ids";
    private static final String FIRST_PRODUCT_IDS = "first_product_ids";
    public static final String FIRST_ORDER_ID_FIELD = "first_order_id";

    public static List<ProductDataHolder> sortProductInfo(List<SimpleRowWrapper> products) {
        return products.stream().collect(HashMap<String, ProductDataHolder>::new, (m, v) -> {
                    String id = v.getString(STRUCT_PRODUCT_ID_FIELD);
                    String name = v.getString(STRUCT_PRODUCT_NAME_FIELD);
                    ProductDataHolder currentStats = m.getOrDefault(id, new ProductDataHolder(id, name));
                    currentStats.incrementCount();
                    currentStats.addValue(v.getDouble(STRUCT_PRODUCT_PRICE_FIELD, 0.0D));
                    currentStats.addCustomerKey(v.getString(USER_ID_FIELD));
                    m.put(id, currentStats);
                }, HashMap::putAll)
                .values()
                .stream()
                .sorted(Comparator.comparing(ProductDataHolder::getCustomerCount).thenComparing(ProductDataHolder::getTotalValue).reversed())
                .collect(Collectors.toList());
    }

    public static List<ProductData> findTopProduct(long segmentCustomers, List<ProductDataHolder> products, int limit) {
        return products.stream()
                .map(p -> new ProductData(p.getName(), p.customerPercentFromTotal(segmentCustomers)))
                .limit(limit)
                .collect(Collectors.toList());
    }

    public static ProductData getLeadingPurchase(long segmentCustomers, List<ProductDataHolder> firstProducts) {
        return firstProducts.stream()
                .findFirst()
                .map(p -> new ProductData(p.getName(), p.customerPercentFromTotal(segmentCustomers)))
                .orElse(null);
    }

    public static SimpleRowWrapper getProductIdsWrapper(Dataset<Row> ordersDataSet, Dataset<Row> productsDataset, Dataset<Row> filteredSegment) {
        Dataset<Row> filteredSegmentAlias = filteredSegment.as("filteredSegment");
        Dataset<Row> ordersDataSetAlias = ordersDataSet.as("ordersDataSet");

        return new SimpleRowWrapper(
                ordersDataSetAlias
                        .join(filteredSegmentAlias, col("ordersDataSet." + USER_ID_FIELD).equalTo(col("filteredSegment." + USER_ID_FIELD)))
                        .join(productsDataset, col("ordersDataSet." + ORDER_PRODUCT_ID_FIELD).equalTo(col(PRODUCT_ID_FIELD)))
                        .agg(
                                collect_list(struct(
                                        col("ordersDataSet." + ORDER_PRODUCT_ID_FIELD).as(STRUCT_PRODUCT_ID_FIELD),
                                        col(PRODUCT_NAME).as(STRUCT_PRODUCT_NAME_FIELD),
                                        col(TOTAL_VALUE_FIELD).as(STRUCT_PRODUCT_PRICE_FIELD),
                                        col("ordersDataSet." + USER_ID_FIELD).as(USER_ID_FIELD)
                                )).as(PRODUCT_IDS)
                                ,
                                collect_list(
                                        when(ordersDataSetAlias.col(ID_FIELD).equalTo(col(FIRST_ORDER_ID_FIELD)),
                                                struct(col("ordersDataSet." + ORDER_PRODUCT_ID_FIELD).as(STRUCT_PRODUCT_ID_FIELD),
                                                        col(PRODUCT_NAME).as(STRUCT_PRODUCT_NAME_FIELD),
                                                        col(TOTAL_VALUE_FIELD).as(STRUCT_PRODUCT_PRICE_FIELD),
                                                        col("ordersDataSet." + USER_ID_FIELD).as(USER_ID_FIELD)
                                                ))).as(FIRST_PRODUCT_IDS)
                        ).collectAsList().get(0));
    }
}
