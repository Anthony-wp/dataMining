package dpl.processing.type;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ShopifyTaskType {
    BUILD_ORDERS_TASK(1),
    BUILD_CONTENTS_TASK(1),
    BUILD_PRODUCTS_TASK(2),
    GENERATE_RESULT(1000),
    ;

    private final Integer order;
}
