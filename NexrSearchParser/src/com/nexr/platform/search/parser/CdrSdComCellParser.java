package com.nexr.platform.search.parser;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.addAll;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 9. 15.
 * Time: 오후 6:21
 * To change this template use File | Settings | File Templates.
 */
public class CdrSdComCellParser {

    private final String FILE_ENCODING="EUC-KR";
    private final String SAVE_ENCODING="UTF-8";

    public void parseData(String read_file_Path, String save_file_path) throws IOException {
        File file = new File(read_file_Path);

        String SEPARATOR = ",";

        if(file.exists()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), FILE_ENCODING));
            // BufferedWriter writer = new BufferedWriter(new FileWriter(save_file_path));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(save_file_path),SAVE_ENCODING));
            String row;
            int rowCount = 0;
            int colCount = 0;

            while((row = reader.readLine()) != null) {
                if(!row.isEmpty()) {
                    if(rowCount != 0) {
                        String[] rows = row.split(SEPARATOR, colCount);

                        if(rows[2].trim().isEmpty()) {
                            rows[2] = rows[0];
                        }

                        for(int i = 0; i < colCount; i++) {
                            writer.append(rows[i]);
                            writer.append(SEPARATOR);
                        }
                        writer.newLine();

                    } else {
                       colCount = row.split(SEPARATOR).length;
                    }
                }

                rowCount++;
            }
        }
    }

    public static void main(String[] args) {

        String save_file_path = "/Users/david/Execute/nexrsearch_client/config/save_sd_com_cell.csv";
        String read_file_path = "/Users/david/Execute/nexrsearch_client/config/sd_com_cell.csv";

        try {
            CdrSdComCellParser parser = new CdrSdComCellParser();
            parser.parseData(read_file_path, save_file_path);
        } catch(Exception e) {
            e.printStackTrace();
        }

    }
}
