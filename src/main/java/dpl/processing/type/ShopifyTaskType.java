package dpl.processing.type;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public enum ShopifyTaskType {
    BUILD_ORDERS_TASK(1),
    BUILD_CONTENTS_TASK(1),
    BUILD_PRODUCTS_TASK(2),
    GENERATE_RESULT(1000),
    ;

    private Integer order;
}
