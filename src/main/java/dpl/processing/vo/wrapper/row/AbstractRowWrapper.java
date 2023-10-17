package dpl.processing.vo.wrapper.row;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.IntFunction;

import static org.apache.spark.sql.types.DataTypes.*;

public abstract class AbstractRowWrapper<S> implements RowWrapper<S> {
    protected static final Map<Class, DataType> JAVA_TO_SPARK_TYPES = ImmutableMap.<Class, DataType>builder()
            .put(String.class, StringType)
            .put(Boolean.class, BooleanType)
            .put(Date.class, DateType)
            .put(Timestamp.class, TimestampType)
            .put(Double.class, DoubleType)
            .put(Float.class, FloatType)
            .put(Byte.class, ByteType)
            .put(Integer.class, IntegerType)
            .put(Long.class, LongType)
            .put(Short.class, ShortType)
            .build();
    protected final Row row;
    protected final Map<Integer, StructField> fieldsByIdx = new HashMap<>();
    protected final Map<String, Integer> idxByName = new HashMap<>();

    protected AbstractRowWrapper(Row row) {
        this.row = row;

        StructField[] structFields = row.schema().fields();
        for (int i = 0; i < structFields.length; i++) {
            StructField structField = structFields[i];
            fieldsByIdx.put(i, structField);
            idxByName.put(structField.name(), i);
        }
    }

    protected abstract String getFieldName(S field);

    protected <T> T getAs(S field, DataType dataType, IntFunction<T> function, T defaultValue) {
        return getIndex(getFieldName(field))
                .filter(idx -> isValidDataField(idx, dataType))
                .map(function::apply)
                .orElse(defaultValue);
    }

    private Optional<Integer> getIndex(String field) {
        return Optional.ofNullable(field).map(idxByName::get);
    }

    private boolean isValidDataField(int idx, DataType dataType) {
        return !row.isNullAt(idx) && fieldsByIdx.get(idx).dataType().equals(dataType);
    }

    @Override
    public Object get(S field, Object defaultValue) {
        return getIndex(getFieldName(field)).map(row::get).orElse(defaultValue);
    }

    @Override
    public Double getDouble(S field, Double defaultValue) {
        return getAs(field, DataTypes.DoubleType, row::getDouble, defaultValue);
    }

    @Override
    public Number getDecimal(S field, Double defaultValue) {
        return getAs(field, DecimalType.apply(38, 2), row::getDecimal, defaultValue);
    }

    @Override
    public Float getFloat(S field, Float defaultValue) {
        return getAs(field, FloatType, row::getFloat, defaultValue);
    }

    @Override
    public Date getDate(S field, Date defaultValue) {
        return getAs(field, DataTypes.DateType, row::getDate, defaultValue);
    }

    @Override
    public Timestamp getTimestamp(S field, Timestamp defaultValue) {
        return getAs(field, DataTypes.TimestampType, row::getTimestamp, defaultValue);
    }

    @Override
    public <T> List<T> getList(S field, Class<T> clazz, List<T> defaultValue) {
        return getAs(field, DataTypes.createArrayType(JAVA_TO_SPARK_TYPES.getOrDefault(clazz, NullType)), row::getList, defaultValue);
    }

    @Override
    public String getString(S field, String defaultValue) {
        return getAs(field, StringType, row::getString, defaultValue);
    }

    @Override
    public Long getLong(S field, Long defaultValue) {
        return getAs(field, DataTypes.LongType, row::getLong, defaultValue);
    }

    @Override
    public Integer getInt(S field, Integer defaultValue) {
        return getAs(field, DataTypes.IntegerType, row::getInt, defaultValue);
    }

    @Override
    public Boolean getBoolean(S field, Boolean defaultValue) {
        return getAs(field, BooleanType, row::getBoolean, defaultValue);
    }

    protected Map<S, Object> collectAsMap(Collection<S> fields) {
        return fields.stream()
                .collect(HashMap::new, (m, v) -> m.put(v, row.get(idxByName.get(getFieldName(v)))), Map::putAll);
    }
}
