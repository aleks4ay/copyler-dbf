package kiyv.domain.tools;

import java.util.Calendar;
import java.util.Date;

public final class DateConverter {
    public static Calendar calendar = Calendar.getInstance();
    public static Calendar calendarAfterPeriod = Calendar.getInstance();


    public static long getNowDate(){
        return new Date().getTime();
    }

    public static int getYear(long millis) {
        calendar.setTimeInMillis(millis);
        return calendar.get(Calendar.YEAR);
    }

    public static int getYearShort(long millis) {
        String year = String.valueOf(getYear(millis));
        return Integer.valueOf(year.substring(2));
    }

    public static long offset (long millisecond, int duration) {
        calendarAfterPeriod.setTimeInMillis(millisecond);
        calendarAfterPeriod.add(Calendar.DATE, Integer.valueOf(duration));
        return calendarAfterPeriod.getTimeInMillis();
    }

/*    public static String dateFromLongToString(long millis) {
        calendar.setTimeInMillis(millis);
        String date = calendar.get(Calendar.DATE);
        calendar.get(Calendar.HOUR);
        calendar.get(Calendar.MINUTE);
    }*/

    public static String dateToString(long millis) {
        if ( (Long)millis == null | millis == 0 ) {
            return "-";
        }
        String date = "";
        calendar.setTimeInMillis(millis);
        int day, month;
        if ((day = calendar.get(Calendar.DAY_OF_MONTH))<10) {
            date += "0" + day + ".";
        }
        else {
            date += "" + day + ".";
        }
        if ((month = calendar.get(Calendar.MONTH) + 1 )<10) {
            date += "0" + month;
        }
        else {
            date += month;
        }
        return date + "." + calendar.get(Calendar.YEAR);
    }

    public static String timeToString(long millis) {
        calendar.setTimeInMillis(millis);
        String time = "";
        int hour, min;
        if ((hour = calendar.get(Calendar.HOUR_OF_DAY))<10) {
            time += "0" + hour + ":";
        }
        else {
            time += hour + ":";
        }

        if ((min = calendar.get(Calendar.MINUTE) )<10) {
            time += "0" + min;
        }
        else {
            time += min;
        }
        return time;
    }
}
