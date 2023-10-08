package dpl.processing.utils;

import lombok.experimental.UtilityClass;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;

@UtilityClass
public class ScalaUtils {
    public static <T> Seq<T> toSeq(List<T> javaList) {
        return JavaConverters.asScalaBufferConverter(javaList).asScala().toSeq();
    }
}
