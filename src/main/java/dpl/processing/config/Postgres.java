package dpl.processing.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class Postgres extends SparkConnectionSource {
    private String url;
    private String user;
    private String password;

    @Override
    public SparkConf getConfigs() {
        return buildDefaultConfiguration()
                .set("url", String.valueOf(url))
                .set("user", String.valueOf(user))
                .set("password", String.valueOf(password))
                .set("driver", "org.postgresql.Driver");
    }
}
