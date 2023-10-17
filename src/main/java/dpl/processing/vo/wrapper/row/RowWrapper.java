package dpl.processing.vo.wrapper.row;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public interface RowWrapper<S> {

    default Object get(S field) {
        return get(field, null);
    }

    default Timestamp getTimestamp(S field) {
        return getTimestamp(field, null);
    }

    default String getString(S field) {
        return getString(field, null);
    }

    default Long getLong(S field) {
        return getLong(field, null);
    }

    default Integer getInt(S field) {
        return getInt(field, null);
    }

    default Boolean getBoolean(S field) {
        return getBoolean(field, null);
    }

    default Double getDouble(S field) {
        return getDouble(field, null);
    }

    default Date getDate(S field) {
        return getDate(field, null);
    }

    default <T> List<T> getList(S field, Class<T> clazz) {
        return getList(field, clazz, null);
    }

    Object get(S field, Object defaultValue);

    Double getDouble(S field, Double defaultValue);

    Number getDecimal(S field, Double defaultValue);

    Float getFloat(S field, Float defaultValue);

    Date getDate(S field, Date defaultValue);

    Timestamp getTimestamp(S field, Timestamp defaultValue);

    <T> List<T> getList(S field, Class<T> clazz, List<T> defaultValue);

    String getString(S field, String defaultValue);

    Long getLong(S field, Long defaultValue);

    Integer getInt(S field, Integer defaultValue);

    Boolean getBoolean(S field, Boolean defaultValue);

    Map<S, Object> collectAsMap();
}
