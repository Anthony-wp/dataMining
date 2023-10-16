package dpl.processing.type;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public enum PostgresAppTables {
    PRODUCT_TABLE("product"),
    ORDER_TABLE("order"),
    USER_TABLE("user");

    private String tableName;

    PostgresAppTables(String tableName) {
        this.tableName = tableName;
    }
}
