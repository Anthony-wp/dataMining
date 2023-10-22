package dpl.processing.vo.holder;

import dpl.processing.model.QuarterStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang.WordUtils;

import java.time.LocalDate;
import java.time.Year;
import java.util.Date;

import static dpl.processing.utils.DateUtils.asDate;

@Data
@AllArgsConstructor
public class YearQuarterHolder {
    private final Date start;
    private final Date end;
    private final String calendarQ;
    private final QuarterStatus status;

    public YearQuarterHolder(LocalDate yearStart, int quarter, Date currentDay) {
        int daysInYear = Year.of(yearStart.getYear()).length();

        LocalDate qStart = yearStart.plusMonths((quarter - 1) * 3);
        LocalDate qEnd = qStart.plusMonths(3);

        this.start = asDate(qStart);
        this.end = asDate(qEnd);
        this.calendarQ = yearStart.getYear() + "/" + (yearStart.getYear() + 1)  + " - Q" + quarter
                + " (" + WordUtils.capitalizeFully(qStart.getMonth().name()).substring(0, 3) + ")";
        this.status = isActualQuarter(currentDay) ? QuarterStatus.ACTUAL : QuarterStatus.FORECAST;
    }

    // start - inclusive, end - exclusive
    public boolean isInQ(Date date) {
        return (start.equals(date) || start.before(date)) && end.after(date);
    }

    public boolean isActualQuarter(Date date) {
        return end.before(date);
    }
}

