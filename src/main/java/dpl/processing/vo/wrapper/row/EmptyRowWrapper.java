package dpl.processing.vo.wrapper.row;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EmptyRowWrapper implements RowWrapper<String> {
    @Override
    public Object get(String field, Object defaultValue) {
        return defaultValue;
    }

    @Override
    public Double getDouble(String field, Double defaultValue) {
        return defaultValue;
    }

    @Override
    public Float getFloat(String field, Float defaultValue) {
        return defaultValue;
    }

    @Override
    public Date getDate(String field, Date defaultValue) {
        return defaultValue;
    }

    @Override
    public Timestamp getTimestamp(String field, Timestamp defaultValue) {
        return defaultValue;
    }

    @Override
    public <T> List<T> getList(String field, Class<T> clazz, List<T> defaultValue) {
        return defaultValue;
    }

    @Override
    public String getString(String field, String defaultValue) {
        return defaultValue;
    }

    @Override
    public Long getLong(String field, Long defaultValue) {
        return defaultValue;
    }

    @Override
    public Integer getInt(String field, Integer defaultValue) {
        return defaultValue;
    }

    @Override
    public Boolean getBoolean(String field, Boolean defaultValue) {
        return defaultValue;
    }

    @Override
    public Map<String, Object> collectAsMap() {
        return Collections.emptyMap();
    }
}
