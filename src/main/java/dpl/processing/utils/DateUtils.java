package dpl.processing.utils;

import org.apache.commons.lang3.text.WordUtils;
import org.joda.time.*;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class DateUtils {
    private static final Pattern BETWEEN_BRACKETS = Pattern.compile("\\((.*)\\)");
    public static final int DAYS_MILLIS = 1000 * 60 * 60 * 24;
    public static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("dd/MM/yyyy");
    public static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter SHOPIFY_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd");
    public static final String DEFAULT_ZAPIER_DATES_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    public static final String TRACKER_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final DateTimeFormatter DEFAULT_UI_DATE_FORMAT = DateTimeFormat.forPattern("dd/MM/yyyy");
    private static final DateFormat DATE_FORMAT_FOR_ALERT_NOTIFICATION = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final java.time.format.DateTimeFormatter HOUR_TIME_KEY_FORMATTER = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
    private static final java.time.format.DateTimeFormatter DAY_TIME_KEY_FORMATTER = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private DateUtils() {
    }

    public static DateTime getEpochDate() {
        MutableDateTime epoch = new MutableDateTime();
        epoch.setDate(0);
        epoch.setHourOfDay(0);
        epoch.setMinuteOfDay(0);

        return epoch.toDateTime();
    }

    public static Date asDate(java.time.LocalDate localDate) {
        return Date.from(localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
    }

    public static Date asDate(LocalDateTime localDateTime) {
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    public static java.time.LocalDate setYearStartRelatively(java.time.LocalDate yearStart, java.time.LocalDate date) {
        if (yearStart.getDayOfYear() > date.getDayOfYear()) {
            return yearStart.withYear(date.getYear() - 1);
        } else {
            return yearStart.withYear(date.getYear());
        }
    }

    public static int numberOfDaysSinceEpoch(DateTime dateTime) {
        return Days.daysBetween(getEpochDate(), dateTime).getDays();
    }

    public static String printDateFromMillis(Object o) {
        return printFromMillis(o, DATE_FORMAT);
    }

    public static String formatFromMillis(Object o, String format) {
        return printFromMillis(o, DateTimeFormat.forPattern(format));
    }

    public static String printDateTimeFromMillis(Object o) {
        return printFromMillis(o, DATE_TIME_FORMAT);
    }

    private static String printFromMillis(Object o, DateTimeFormatter format) {
        if (o == null) {
            return null;
        }

        return NumberUtils.parseLong(o.toString()).map(format::print).orElse(null);
    }

    @SafeVarargs
    public static <T extends Comparable<T>> Optional<T> max(T... values) {
        return Stream.of(values).filter(Objects::nonNull).max(Comparable::compareTo);
    }

    public static LocalDate parseShopifyLocalDate(String date) {
        try {
            return LocalDate.parse(date, SHOPIFY_DATE_FORMAT);
        } catch (Exception e) {
            throw new IllegalArgumentException("Incorrect date format! Must be a 'yyyy-MM-dd'. Got '" + date + "'");
        }
    }

    public static String toShopifyFormat(LocalDate date) {
        try {
            return SHOPIFY_DATE_FORMAT.print(date);
        } catch (Exception e) {
            throw new IllegalArgumentException("Incorrect date format! Must be a 'yyyy-MM-dd'. Got '" + date + "'");
        }
    }

    public static String parseDateToString(Date date, String format) {
        try {
            return new SimpleDateFormat(format).format(date);
        } catch (Exception e) {
            return null;
        }
    }

    public static DateTime parseStringDateToJodaDate(String date, String format) {
        try {
            return DateTime.parse(date, DateTimeFormat.forPattern(format));
        } catch (Exception e) {
            return null;
        }
    }

    public static Boolean doesDateParseForFormat(String date, String format) {
        try {
            DateTime.parse(date, DateTimeFormat.forPattern(format));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static ZoneId getZoneIdForShopifyTimezone(String shopifyTimeZone) {
        if(StringUtils.isEmpty(shopifyTimeZone)) {
            return ZoneId.systemDefault();
        }
        Matcher matcher = BETWEEN_BRACKETS.matcher(shopifyTimeZone);
        if (matcher.find()) {
            String timeZone = matcher.group(1).replace(" ", "");

            return TimeZone.getTimeZone(timeZone).toZoneId();
        } else {
            return ZoneId.systemDefault();
        }
    }

    public static String getDateWithMonthSuffix(LocalDateTime dateTime) {
        int day = dateTime.getDayOfMonth();
        if (day >= 11 && day <= 13) {
            return String.format("%d%s %s", day, "th", WordUtils.capitalizeFully(dateTime.getMonth().name()));
        }
        switch (day % 10) {
            case 1:
                return String.format("%d%s %s", day, "st", WordUtils.capitalizeFully(dateTime.getMonth().name()));
            case 2:
                return String.format("%d%s %s", day, "nd", WordUtils.capitalizeFully(dateTime.getMonth().name()));
            case 3:
                return String.format("%d%s %s", day, "rd", WordUtils.capitalizeFully(dateTime.getMonth().name()));
            default:
                return String.format("%d%s %s", day, "th", WordUtils.capitalizeFully(dateTime.getMonth().name()));
        }
    }

    public static DateTime parseDateToJodaDate(Date date, String format) {
        try{
            return parseStringDateToJodaDate(parseDateToString(date, format), format);
        } catch (Exception e) {
            return null;
        }
    }

    public static java.sql.Date toSqlDate(Date date) {
        if (date == null) {
            return null;
        }

        return new java.sql.Date(date.getTime());
    }

    public static Date fromDefaultUIDateFormat(String value) {
        return DEFAULT_UI_DATE_FORMAT.parseDateTime(value).toDate();
    }

    public static String getFormatDateForAlertNotification(Date date) {
        return DATE_FORMAT_FOR_ALERT_NOTIFICATION.format(date);
    }

    public static String normalizeTime(Long value) {
        return String.valueOf(value).length() < 2 ? "0" + value : String.valueOf(value);
    }

    public static int getNumberOfHoursBetweenDates(DateTime start, DateTime end) {

        if (start.isAfter(end)) {
            return 0;
        }

        return Hours.hoursBetween(start, end).getHours();
    }

    public static int getNumberOfHoursFromNow(DateTime start) {
        return getNumberOfHoursBetweenDates(start, DateTime.now());
    }

    public static int getNumberOfHoursFromNowMinusOne(DateTime start) {
        return getNumberOfHoursBetweenDates(start, DateTime.now().minusHours(1));
    }

    public static java.time.LocalDate asLocalDate(Date date) {
        return java.time.Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate();
    }

    public static LocalDateTime asLocalDateTime(Date date) {
        return java.time.Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    public static int weekNumberInFinancialYear(java.time.LocalDate yearStart, java.time.LocalDate date) {
        long daysBetween = ChronoUnit.DAYS.between(setYearStartRelatively(yearStart, date), date);
        return (int) (1 + daysBetween / 7 + (int) Math.signum(daysBetween % 7));
    }

    public static List<String> generateHourTimeKeys(LocalDateTime startDateTime, long hoursToThePast) {
        return LongStream.range(0, hoursToThePast + 1)
                .mapToObj(startDateTime::minusHours)
                .map(HOUR_TIME_KEY_FORMATTER::format)
                .collect(Collectors.toList());
    }

    public static String generateDayTimeKey(LocalDateTime startDateTime, long dayToThePast) {
        return DAY_TIME_KEY_FORMATTER.format(startDateTime.minusDays(dayToThePast));
    }

    public static List<String> generateDayTimeKeys(LocalDateTime startDateTime, long dayToThePast) {
        return LongStream.range(0, dayToThePast + 1)
                .mapToObj(startDateTime::minusDays)
                .map(DAY_TIME_KEY_FORMATTER::format)
                .collect(Collectors.toList());
    }

    public static List<Timestamp> generateWeekTimeKeys(LocalDateTime startDateTime, LocalDateTime endDateTime) {
        return LongStream.range(0, ChronoUnit.WEEKS.between(startDateTime, endDateTime) + 1)
                .mapToObj(startDateTime::plusWeeks)
                .map(Timestamp::valueOf)
                .collect(Collectors.toList());
    }
}
