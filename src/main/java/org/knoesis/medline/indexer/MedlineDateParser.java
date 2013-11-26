
package org.knoesis.medline.indexer;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author alan
 */
public class MedlineDateParser {
    
    private static final Map<String, Integer> SEASON_TO_MONTH = new HashMap<String, Integer>() {{
        put("Spring", Calendar.MARCH);
        put("Summer", Calendar.JUNE);
        put("Fall", Calendar.SEPTEMBER);
        put("Winter", Calendar.DECEMBER);
    }};
    
    private static final Map<String, Integer> MONTH_TO_INT = new HashMap<String, Integer>() {{
        put("Jan", Calendar.JANUARY);
        put("Feb", Calendar.FEBRUARY);
        put("Mar", Calendar.MARCH);
        put("Apr", Calendar.APRIL);
        put("May", Calendar.MAY);
        put("Jun", Calendar.JUNE);
        put("Jul", Calendar.JULY);
        put("Aug", Calendar.AUGUST);
        put("Sep", Calendar.SEPTEMBER);
        put("Oct", Calendar.OCTOBER);
        put("Nov", Calendar.NOVEMBER);
        put("Dec", Calendar.DECEMBER);
    }};
    
    private static final Pattern MONTH;
    static {
        StringBuilder sb = new StringBuilder("(");
        for (String month : MONTH_TO_INT.keySet()) {
            if (sb.length() > 1) {
                sb.append("|");
            }
            sb.append(month);
        }
        sb.append(")");
        MONTH = Pattern.compile(sb.toString());
    }
    
    private static final Pattern PATTERN = Pattern.compile("(\\d{4})(?: ([a-zA-Z-]+)(?: (\\d{1,2}))?)?");
    
    private MedlineDateParser(){}
    
    public static Calendar parseAsCalendar(String dateStr) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        
        Matcher matcher = PATTERN.matcher(dateStr);
        if (!matcher.find()) {
            return calendar;
        }
        calendar.set(Calendar.YEAR, getYear(matcher));
        calendar.set(Calendar.MONTH, getMonth(matcher));
        calendar.set(Calendar.DAY_OF_MONTH, getDay(matcher));
        return calendar;
    }
    
    public static long parseAsLong(String dateStr) {
        return parseAsCalendar(dateStr).getTimeInMillis();
    }
    
    public static Date parseAsDate(String dateStr) {
        return parseAsCalendar(dateStr).getTime();
    }
    
    private static int getYear(Matcher matcher) {
        return Integer.parseInt(matcher.group(1));
    }
    
    private static int getMonth(Matcher matcher) {
        String monthStr = matcher.group(2);
        if (monthStr != null) {
            if (SEASON_TO_MONTH.containsKey(monthStr)) {
                return SEASON_TO_MONTH.get(monthStr);
            }
            Matcher monthMatcher = MONTH.matcher(monthStr);
            if (monthMatcher.find()) {
                String monthKey = monthMatcher.group();
                if (MONTH_TO_INT.containsKey(monthKey)) {
                    return MONTH_TO_INT.get(monthKey);
                }
            }
        }
        return Calendar.JANUARY;
    }
    
    private static int getDay(Matcher matcher) {
        String dayStr = matcher.group(3);
        if (dayStr != null) {
            return Integer.parseInt(dayStr);
        }
        return 1;
    }

}

