package com.nexr.platform.search.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 7/29/11
 * Time: 2:00 PM
 */
public class DateUtils {

    private SimpleDateFormat _simpleDateFormat;
    private Calendar _calendar;
    private Date _date;
    private String _dateFormat;


    public DateUtils(String dateFormat) {
        _calendar = Calendar.getInstance();
        _date = new Date();

        setDateFormat(dateFormat);
    }

    public String getDateFormat() {
        return _dateFormat;
    }

    public String getCurrentTime(){
        _date = new Date();
        return _simpleDateFormat.format(_date);
    }

    public void setDateFormat(String dateFormat) {
        _simpleDateFormat = new SimpleDateFormat(dateFormat);
        _dateFormat = dateFormat;
    }

    /**
     * 세팅 된 Date 에 달을 더하 거나 뺀다.
     * @param intSecond  더하 거나 뺄 초의 수.
     * @return  초를 더하 거나 뺀 날짜.
     */
    public String getAddSecond(int intSecond){
        _calendar.add(Calendar.SECOND , intSecond);
        _date = _calendar.getTime();
        return _simpleDateFormat.format(_date);
    }

    /**
     * 세팅 된 Date 에 달을 더하 거나 뺀다.
     * @param intMonth  더하 거나 뺄 달의 수.
     * @return  달을 더하 거나 뺀 날짜.
     */
    public String getAddMonth(int intMonth){
        _calendar.setTime(_date);
        _calendar.add(Calendar.MONTH, intMonth);
        _date = _calendar.getTime();
        return _simpleDateFormat.format(_date);
    }

    /**
     * 세팅 된 Date 에 일을 더하 거나 뺀다.
     * @param intDay  더하 거나 뺄 일의 수.
     * @return  일을 더하 거나 뺀 날짜.
     */
    public String getAddDay(int intDay){
        _calendar.setTime(_date);
        _calendar.add(Calendar.DATE, intDay);
        _date = _calendar.getTime();
        return _simpleDateFormat.format(_date);
    }



}
