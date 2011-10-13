package com.panelion.utils;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 8. 15.
 * Time: 오전 11:59
 */
public class ValidateUtils {

    public static String getValidValue(String value) {
        return isNull(value) ? "" : value.trim();
    }

    public static boolean isNull(String value) {
        return value == null || value.trim().length() == 0 || value.equalsIgnoreCase("null") || value.equalsIgnoreCase("\\N");
    }

}
