package com.nexr.platform.search.util;

import com.nexr.platform.search.entity.SCKeyEntity;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.addAll;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 8. 19.
 * Time: 오후 2:11
 */
public class CheckTheCdrData {

    private final String _ENCODING = "EUC-KR";

    public Map<SCKeyEntity, Map<String, String>> loadSCFileData(String filePath) throws IOException {
        File file = new File(filePath);

        String SEPARATOR = ",";

        Map<SCKeyEntity, Map<String, String>> joinMap = new HashMap<SCKeyEntity, Map<String, String>>();

        if(file.exists()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), _ENCODING));
            String row;
            int rowCount = 0;

            ArrayList<String> columnDefine = new ArrayList<String>();

            Map<String, String> map;
            while((row = reader.readLine()) != null) {
                if(!row.isEmpty()) {

                    map = new HashMap<String, String>();
                    String[] rows;

                    if(rowCount == 0) {
                        rows = row.split(SEPARATOR);
                        addAll(columnDefine, rows);
                    } else {
                        rows = row.trim().split(SEPARATOR, columnDefine.size());
                        for(int i = 0; i < columnDefine.size(); i++) {
                            try {
                                map.put(columnDefine.get(i), rows[i].trim() == null ? "" : rows[i].trim());
                            } catch(Exception e) {
                                map.put(columnDefine.get(i), "");
                            }
                        }
                    }

                    if(rowCount != 0) {
                        SCKeyEntity scKey = new SCKeyEntity(map.get("I_SWITCH"), map.get("I_BSC"), map.get("I_CELL"), map.get("I_ENDT"));
                        joinMap.put(scKey, map);

                        if(map.get("U_CELL").equals("개포2-K")) {
                            System.out.println(map.get("U_CELL") + " = " + map.get("I_SWITCH") + "|" + map.get("I_BSC") + "|" + map.get("I_CELL") + "|" + map.get("I_ENDT"));
                        }
                   }
                    rowCount++;
                }
            }
        }
        return joinMap;
    }

    public static void main(String[] args) {
        String sdComCellFilePath = "/Users/david/Execute/nexrsearch_client/config/sd_com_cell.csv";

        File file = new File(sdComCellFilePath);

        CheckTheCdrData checkTheCdrData = new CheckTheCdrData();
        Map<SCKeyEntity, Map<String, String>> mapData = null;
        try {
            mapData = checkTheCdrData.loadSCFileData(sdComCellFilePath);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "EUC-KR"));
            String row;
            int rowCount = 0;
            String SEPARATOR = ",";

            ArrayList<String> columnDefine = new ArrayList<String>();

            while((row = reader.readLine()) != null) {
                 if(!row.isEmpty()) {

                    Map<String, String> map = new HashMap<String, String>();
                    String[] rows;

                    if(rowCount == 0) {
                        rows = row.split(SEPARATOR);
                        addAll(columnDefine, rows);
                    } else {
                        if(!row.isEmpty()){
                            rows = row.trim().split(SEPARATOR, columnDefine.size());
                            String i_switch="", i_bsc="", i_cell="", u_cell="", i_endt="";
                            for(int i = 0; i < columnDefine.size(); i++) {
                                try {
                                    String colName = columnDefine.get(i);
                                    if(colName.equalsIgnoreCase("i_switch")) {
                                        i_switch = rows[i].trim();
                                    } else if(colName.equalsIgnoreCase("i_bsc")) {
                                        i_bsc = rows[i].trim();
                                    } else if(colName.equalsIgnoreCase("i_cell")) {
                                        i_cell = rows[i].trim();
                                    } else if(colName.equalsIgnoreCase("u_cell")) {
                                        u_cell = rows[i].trim();
                                    } else if(colName.equalsIgnoreCase("i_endt")) {
                                        i_endt = rows[i].trim();
                                    }
                                } catch(Exception e) {
                                    e.printStackTrace();
                                }
                            }

                                SCKeyEntity entity = new SCKeyEntity(i_switch, i_bsc, i_cell, i_endt);


                                Map<String, String> rtnData = null;

                                if(mapData.containsKey(entity)) {
                                    rtnData = mapData.get(entity);
                                }

                                if(rtnData != null) {
                                    String first_u_cell = rtnData.get("U_CELL");

                                    if(!first_u_cell.equals(u_cell))  {
                                        System.out.println("first u_cell = " + first_u_cell);
                                        System.out.println("second u_cell = " + u_cell);
                                    }
                                } else {
                                    System.out.println("second u_cell = " + u_cell);
                                }
                        }
                    }
                    rowCount++;
                }
            }


        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }
}
