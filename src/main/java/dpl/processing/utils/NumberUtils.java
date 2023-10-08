package dpl.processing.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Optional;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NumberUtils {

    public static Optional<Integer> parseInteger(String i) {
        try {
            return Optional.of(Integer.parseInt(i));
        } catch (NumberFormatException nfe) {
            return Optional.empty();
        }
    }

    public static boolean isInRangeInc(Integer base, Integer lowerBound, Integer upperBound) {
        return base != null &&
                Optional.ofNullable(lowerBound).map(l -> l <= base).orElse(true) &&
                Optional.ofNullable(upperBound).map(u -> u <= base).orElse(true);

    }

    public static Optional<Long> parseLong(String l) {
        try {
            return Optional.of(Long.parseLong(l));
        } catch (NumberFormatException nfe) {
            return Optional.empty();
        }
    }

    public static Optional<Double> parseDouble(String d) {
        try {
            return Optional.of(Double.parseDouble(d));
        } catch (NumberFormatException nfe) {
            return Optional.empty();
        }
    }

}
