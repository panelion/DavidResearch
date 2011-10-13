package com.nexr.platform.search.david;

import com.panelion.utils.FileUtils;
import org.elasticsearch.common.io.FastByteArrayOutputStream;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: david
 * Date: 11. 10. 11.
 * Time: 오후 5:30
 */
public class PerformanceJsonVsSmile {

    public interface Builder {

        public XContentBuilder generator();

        public Map<String, Object> parse(XContentBuilder builder);

        public void print();
    }

    private enum CONTENT_TYPE {
        JSON,
        SMILE
    }

    public static Builder getInstance(CONTENT_TYPE TYPE) {

        switch (TYPE) {
            case JSON :
                return new JsonBuilder();
            case SMILE:
                return new SmileBuilder();
            default:
                return null;
        }
    }

    private static class JsonBuilder implements Builder {
        @Override
        public XContentBuilder generator() {
            XContentBuilder builder = null;
            try {
                FastByteArrayOutputStream jsonOs = new FastByteArrayOutputStream();
                builder =  XContentFactory.jsonBuilder(jsonOs);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return builder;
        }

        @Override
        public Map<String, Object> parse(XContentBuilder builder) {
            try {
                return XContentFactory.xContent(XContentType.JSON).createParser(builder.copiedBytes()).mapAndClose();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public void print() {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    private static class SmileBuilder implements Builder {

        @Override
        public XContentBuilder generator() {
            XContentBuilder builder = null;
            try {
                FastByteArrayOutputStream smileOs = new FastByteArrayOutputStream();
                builder =  XContentFactory.smileBuilder(smileOs);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return builder;
        }

        @Override
        public Map<String, Object> parse(XContentBuilder builder) {
            try {
                return XContentFactory.xContent(XContentType.SMILE).createParser(builder.copiedBytes()).mapAndClose();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public void print() {
        }
    }

    private static String print(int dataCount, long jsonGenerateTime, long smileGenerateTime, long jsonParseTime, long smileParseTime) {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(dataCount).append("\t");
        stringBuilder.append(jsonGenerateTime).append("\t");
        stringBuilder.append(smileGenerateTime).append("\t");
        stringBuilder.append(jsonParseTime).append("\t");
        stringBuilder.append(smileParseTime).append("\t");

        return stringBuilder.toString();
    }

    private static Map<String, Object> generateMapData(String filePath) {

        Map<String, Object> mapData = new HashMap<String, Object>();

        try {
            FileUtils fileUtils = new FileUtils(filePath);
            fileUtils.setReadable("UTF-8");

            String line;

            while((line = fileUtils.getReadLine()) != null) {
                String[] cols = new String[2];
                cols = line.split("\t", cols.length);

                mapData.put(cols[0], cols[1]);
                line = null;
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        return mapData;
    }

    public static void main(String[] args) throws IOException {
        Builder jsonBuilder = PerformanceJsonVsSmile.getInstance(CONTENT_TYPE.JSON);
        Builder smileBuilder = PerformanceJsonVsSmile.getInstance(CONTENT_TYPE.SMILE);

        // String filePath = "/Users/david/Execute/Data/test/sd_com_cell_sample_data.csv";
        String dataFilePath = "/Users/david/Execute/nexrsearch_client/config/sd_com_cell.csv";
        // Map<String, Object> mapData = PerformanceJsonVsSmile.generateMapData(filePath);

        FileUtils fileUtils = new FileUtils(dataFilePath);
        fileUtils.setReadable("UTF-8");

        // FileUtils writeFileUtils = new FileUtils("/Users/david/Execute/nexrsearch_client/config/jsonVsSmile.txt");
        // writeFileUtils.setWritable(true);

        List<Map<String, Object>> dataList = fileUtils.getHorizonTableFile("UTF-8", ",");

        int dataCount = 1;
        for (Map<String, Object> mapData : dataList) {
            if (mapData != null) {

                XContentBuilder jsonGenerator = jsonBuilder.generator();
                XContentBuilder smileGenerator = smileBuilder.generator();

                System.out.println("------------------------------------------------------------");
                //TimeChecker.startTime("generate json data");
                long jsonGenerateStartTime = System.nanoTime();
                XContentBuilder jsonGenerateBuilder = jsonGenerator.map(mapData);
                long jsonGenerateEndTime = System.nanoTime();
                //TimeChecker.endTime("generate json data");

                // System.out.println(new String(jsonGenerateBuilder.copiedBytes()));

                //TimeChecker.startTime("generate smile data");
                long smileGenerateStartTime = System.nanoTime();
                XContentBuilder smileGenerateBuilder = smileGenerator.map(mapData);
                long smileGenerateEndTime = System.nanoTime();
                //TimeChecker.endTime("generate smile data");

                //TimeChecker.startTime("parse json data");
                long jsonParseStartTime = System.nanoTime();
                jsonBuilder.parse(jsonGenerateBuilder);
                long jsonParseEndTime = System.nanoTime();
                //TimeChecker.endTime("parse json data");

                // System.out.println(new String(smileGenerateBuilder.copiedBytes()));

                // TimeChecker.startTime("parse smile data");
                long smileParseStartTime = System.nanoTime();
                smileBuilder.parse(smileGenerateBuilder);
                long smileParseEndTime = System.nanoTime();
                // TimeChecker.endTime("parse smile data");

                jsonGenerateBuilder.close();
                smileGenerateBuilder.close();

                jsonGenerator.close();
                smileGenerator.close();

                BufferedWriter out = new BufferedWriter(new FileWriter("/Users/david/Execute/nexrsearch_client/config/out.txt", true));

                String result = print(dataCount, (jsonGenerateEndTime - jsonGenerateStartTime), (smileGenerateEndTime - smileGenerateStartTime),
                        (jsonParseEndTime - jsonParseStartTime), (smileParseEndTime - smileParseStartTime));

                out.write(result);
                out.newLine();

                out.close();

                dataCount++;
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }


        fileUtils.close();
        // writeFileUtils.close();
    }

}
