package dpl.processing.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.vladmihalcea.hibernate.type.basic.PostgreSQLEnumType;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;


@Entity
@Inheritance
@Table(schema = "testshop", name = "result_data")
@Builder
@TypeDefs(
        {
                @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class),
                @TypeDef(name = "pgsql_enum", typeClass = PostgreSQLEnumType.class)
        }
)
@NoArgsConstructor
@AllArgsConstructor
public class ResultData implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", updatable = false, nullable = false)
    private Long id;

    @Column(name = "created_date")
    private Date createdDate;

    @Type(type = "jsonb")
    @Column(name = "aggregated_data", columnDefinition = "jsonb")
    private AggregatedData aggregatedData;

    public ResultData(Date createdDate, AggregatedData aggregatedData) {
        this.createdDate = createdDate;
        this.aggregatedData = aggregatedData;
    }

}

