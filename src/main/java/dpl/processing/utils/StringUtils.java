package dpl.processing.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StringUtils {

    public static final String EMPTY_STRING = "";
    public static final String NUll_STRING = "null";
    public static final String DOT_STRING = ".";
    public static final String LODASH_STRING = "_";

    public static boolean isNotEmpty(String str) {
        return !(str == null || EMPTY_STRING.equals(str));
    }

    public static boolean isEmpty(String str) {
        return (str == null || EMPTY_STRING.equals(str));
    }

    public static String emptyStringIfNull(String str) {
        if (str == null || EMPTY_STRING.equals(str)) {
            return EMPTY_STRING;
        }
        return str;
    }

    public static String toString(Object str) {
        return str == null ? null : str.toString();
    }

    public static boolean equals(String str1, String str2) {
        if (str1 == null || str2 == null) {
            return false;
        }
        return str1.equals(str2);
    }

    public static String trimAndLowercase(String str) {
        if (isEmpty(str)) {
            return str;
        }
        return str.trim().toLowerCase();
    }

    public static String trim(String str) {
        if (str == null || EMPTY_STRING.equals(str)) {
            return null;
        }
        return str.trim();
    }

    public static String trimAndUppercase(String str) {
        if (isEmpty(str)) {
            return str;
        }
        return str.trim().toUpperCase();
    }

    public static String concatToField(Object... str) {
        return concatIfNotEmpty(DOT_STRING, str);
    }

    public static String concatToColumn(Object... str) {
        return concatIfNotEmpty(LODASH_STRING, str);
    }

    public static String concatIfNotEmpty(String separator, Object... str) {
        return Stream.of(str)
                .filter(Objects::nonNull).map(Object::toString)
                .filter(v -> !org.springframework.util.StringUtils.isEmpty(v))
                .collect(Collectors.joining(separator));
    }

    public static String cutString(String str, Integer charactersCount) {
        if(str == null) {
            return null;
        }

        return str.substring(0, Math.min(charactersCount, str.length()));
    }
}
