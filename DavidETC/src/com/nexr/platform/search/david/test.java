package com.nexr.platform.search.david;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 11. 2.
 * Time: 오전 9:36
 * To change this template use File | Settings | File Templates.
 */
public class test {
    public static void main(String[] args) {
        SimpleDateFormat sdfCurrent = new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss");
        Timestamp currentTime = new Timestamp(Long.parseLong("7466277355199999"));
        String today = sdfCurrent.format(currentTime);
        System.out.println("today==="+today);
    }
}
