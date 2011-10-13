package com.panelion.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 10. 11.
 * Time: 오후 6:26
 * To change this template use File | Settings | File Templates.
 */
public class TimeChecker {

    public static Map<String, Long> mapTimer = new HashMap<String, Long>();

    public static void startTime(String timerName) {

        if(mapTimer.containsKey(timerName)) {
           mapTimer.remove(timerName);
        }

        long startTime = System.nanoTime();
        mapTimer.put(timerName, startTime);
    }

    public static void endTime(String timerName) {
        long startTime = 0L;
        long endTime = 0L;

        if(mapTimer.containsKey(timerName)) {
            startTime = mapTimer.get(timerName);
            endTime = System.nanoTime();

            mapTimer.remove(timerName);
            System.out.println("[" + timerName + "] : " + (endTime - startTime) + " ms");
        }
    }
}
