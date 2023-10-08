package dpl.processing.vo.wrapper.row;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import scala.collection.convert.Wrappers;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SimpleRowWrapper extends AbstractRowWrapper<String> {
    public SimpleRowWrapper(Row row) {
        super(row);
    }

    @Override
    protected String getFieldName(String field) {
        return field;
    }

    @Override
    public Map<String, Object> collectAsMap() {
        return collectAsMap(idxByName.keySet());
    }

    public List<Map<String, Object>> getListOfMaps(String field, List<Map<String, Object>> defaultValue) {
        DataType fieldType = fieldsByIdx.get(idxByName.get(field)).copy$default$2();

        if (!(fieldType instanceof ArrayType)) {
            return defaultValue;
        }

        return (List<Map<String, Object>>) ((Wrappers.SeqWrapper) getAs(field, fieldType, row::getList, defaultValue))
                .stream()
                .map(innerRow -> new SimpleRowWrapper((Row) innerRow))
                .map(r -> ((SimpleRowWrapper) r).collectAsMap())
                .collect(Collectors.toList());
    }

    public List<Map<String, Object>> getListOfMaps(String field) {
        return getListOfMaps(field, null);
    }

    public List<SimpleRowWrapper> getListOfStructs(String field, boolean emptyListAsDefault) {
        DataType fieldType = fieldsByIdx.get(idxByName.get(field)).copy$default$2();

        List<SimpleRowWrapper> defaultValue = emptyListAsDefault ? Collections.emptyList() : null;

        if (!(fieldType instanceof ArrayType)) {
            return defaultValue;
        }

        return (List<SimpleRowWrapper>) ((Wrappers.SeqWrapper) getAs(field, fieldType, row::getList, defaultValue))
                .stream()
                .map(innerRow -> new SimpleRowWrapper((Row) innerRow))
                .collect(Collectors.toList());
    }

    public List<SimpleRowWrapper> getListOfStructs(String field) {
        return getListOfStructs(field, false);
    }

}
