package com.nexr.platform.search.result;

import com.nexr.platform.search.result.utils.Chart;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 10. 12.
 * Time: 오후 1:56
 */
public class GenerateGraph {

    public void generate(String logFilePath, String saveImgFilePath, String chartName, int width, int height, String[] colsDefine, String colSeparator) {
        XYSeriesCollection dataSet = new XYSeriesCollection();

        for(XYSeries series : this.makeDataSet(logFilePath, colSeparator, colsDefine)){
            dataSet.addSeries(series);
        }

        try {
            Chart chart = new Chart(dataSet, chartName, colsDefine.length);
            chart.save(saveImgFilePath, width, height);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private List<XYSeries> makeDataSet(String localFilePath, String colSeparator, String[] colsDefines) {

        List<XYSeries> list = new ArrayList<XYSeries>();
        try {
            File file = new File(localFilePath);

            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));

            String row;
            while((row = reader.readLine()) != null){
                if(!row.isEmpty()) {
                    String[] cols = row.split(colSeparator);

                    double x_value = Double.parseDouble(cols[0]);
                    for(int i = 1 ; i < cols.length; i++) {
                        if(list.size() != (cols.length - 1)) {
                            list.add(new XYSeries(colsDefines[i - 1]));
                        }

                        if(Integer.parseInt(cols[i]) > 1000000) {
                            System.out.println(Integer.parseInt(cols[i]));
                        }
                        list.get(i - 1).add(x_value, Integer.parseInt(cols[i]));
                    }
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

        return list;
    }

    public static void main(String[] args) {
        String logFilePath = "/Users/david/Execute/nexrsearch_client/config/out.txt";
        String saveImgFilePath = "/Users/david/Execute/nexrsearch_client/config/result.png";
        String chartName = "JSON Vs SMILE";

        int width = 1000;
        int height = 2000;

        String[] colsDefine = {
            "JSON GENERATE",
            "SMILE GENERATE",
            "JSON PARSE",
            "SMILE PARSE"
        };

        String colSeparator = "\t";


        GenerateGraph generateGraph = new GenerateGraph();
        generateGraph.generate(logFilePath, saveImgFilePath, chartName, width, height, colsDefine, colSeparator);
    }
}
