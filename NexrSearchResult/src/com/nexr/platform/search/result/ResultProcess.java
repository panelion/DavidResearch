package com.nexr.platform.search.result;

import com.nexr.platform.search.result.entity.ServerInfoEntity;
import com.nexr.platform.search.result.utils.Chart;
import com.nexr.platform.search.result.utils.FtpClient;

import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.io.*;
import java.util.ArrayList;

public class ResultProcess {

    private ArrayList<ServerInfoEntity> serverList;

    private final String _colSeparator = "\t";
    private final String _logFileExtension = ".log";
    private final String _ENCODING = "EUC-KR";
    private final String _PNG_EXTENSION = ".png";

    private final String _logFileName;
    private final String _saveDirPath;
    private final String _remoteLogDirPath;
    private final boolean _isAverage;

    public ResultProcess(String hostInfoFile, String logFileName, String saveDirPath, String remoteLogDirPath, boolean isAverage) throws IOException {

        /**
         * download 할 서버 정보를 저장 한다.
         */
        serverList = new ArrayList<ServerInfoEntity>();

        File file = new File(hostInfoFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), _ENCODING));
        String row;

        while((row = reader.readLine()) != null){
            if(!row.isEmpty()){
                String[] cols = row.split(_colSeparator);
                if(cols != null && cols.length > 0) {
                    ServerInfoEntity entity = new ServerInfoEntity();

                    entity.setHostName(cols[0]);
                    entity.setId(cols[1]);
                    entity.setPassword(cols[2]);

                    if(cols.length == 4) entity.setPromptString(cols[3]);

                    serverList.add(entity);




                }
            }
        }

        _logFileName = logFileName;
        _saveDirPath = saveDirPath;
        _remoteLogDirPath = remoteLogDirPath;
        _isAverage = isAverage;
    }

    public void process(String saveFilePath, int width, int height) {

        FtpClient ftpClient;
        Chart chart;
        XYSeriesCollection dataSet = new XYSeriesCollection();

        for(ServerInfoEntity entity : serverList){

            System.out.println("---------------------------------------------------------");
            System.out.println("[START " + entity.getHostName() + "]");
            System.out.println("---------------------------------------------------------");
            try {
                ftpClient = new FtpClient(entity.getHostName(), entity.getId(), entity.getPassword());

                String remoteFilePath = _remoteLogDirPath + _logFileName + _logFileExtension;
                String localFilePath = _saveDirPath + _logFileName + "_" + entity.getHostName() + "_" + _logFileExtension;

                // 1. 파일을 로컬로 download 한다.
                if(!ftpClient.get(remoteFilePath, localFilePath)){
                    System.out.println("Remote Log file not Exists : " + remoteFilePath);
                    continue;
                }

                // 2. Chart 에 저장할 DataSet 을 생성 한다.
                dataSet.addSeries(this.makeDataSet(localFilePath, entity));

            } catch(Exception e) {
                System.err.println("can't find file : " + entity.getHostName());
                e.printStackTrace();
            }
        }

        try {
            String saveFullFilePath = saveFilePath + "/" + _logFileName + _PNG_EXTENSION;

            chart = new Chart(dataSet, _logFileName, serverList.size());
            chart.save(saveFullFilePath, width, height);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void processIndividual(String saveFilePath, int width, int height) {

        FtpClient ftpClient;
        Chart chart;

        for(ServerInfoEntity entity : serverList){

            System.out.println("---------------------------------------------------------");
            System.out.println("[START " + entity.getHostName() + "]");
            System.out.println("---------------------------------------------------------");

            try {

                XYSeriesCollection dataSet = new XYSeriesCollection();
                ftpClient = new FtpClient(entity.getHostName(), entity.getId(), entity.getPassword());

                String remoteFilePath = _remoteLogDirPath + _logFileName + _logFileExtension;
                String localFilePath = _saveDirPath + _logFileName + "_" + entity.getHostName() + "_" + _logFileExtension;

                // 1. 파일을 로컬로 download 한다.
                if(!ftpClient.get(remoteFilePath, localFilePath)){
                    System.out.println("Remote Log file not Exists : " + remoteFilePath);
                    continue;
                }

                // 2. Chart 에 저장할 DataSet 을 생성 한다.
                dataSet.addSeries(this.makeDataSet(localFilePath, entity));

                String saveFullFilePath = saveFilePath + "/" + _logFileName + "_" + entity.getHostName() + _PNG_EXTENSION;

                chart = new Chart(dataSet, _logFileName, serverList.size());
                chart.save(saveFullFilePath, width, height);

            } catch(Exception e) {
                System.err.println("can't find file : " + entity.getHostName());
                e.printStackTrace();
            }
        }
    }

    private XYSeries makeDataSet(String localFilePath, ServerInfoEntity entity) {

        String chartName = entity.getHostName();
        XYSeries series = new XYSeries(chartName);

        try {
            File file = new File(localFilePath);

            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), _ENCODING));

            String row;
            while((row = reader.readLine()) != null){
                if(!row.isEmpty()) {
                    double x = 0.0D , y = 0.0D;
                    String[] cols = row.split(_colSeparator);

                    if(cols.length > 4) {
                        try {
                            if(_isAverage)
                                y = Double.parseDouble(cols[3]);
                            else
                                y = Double.parseDouble(cols[2]);

                            x = Double.parseDouble(cols[0]) / (1000.0 * 60.0 * 60.0);

                            series.add(x, y);

                        } catch(Exception e) {
                            System.err.println("arrayIndex Error");
                        }
                    }
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

        return series;
    }

    public static void main(String[] args){

        String hostInfoFile, logFileName, saveDirPath, remoteLogDirPath, saveChartFilePath;
        boolean isAverage = false;
        boolean isSaveImageIndividual = true;

        int width, height;

        if(args.length > 0 ){
            hostInfoFile = args[0];
            logFileName = args[1];
            saveDirPath = args[2];
            remoteLogDirPath = args[3];
            saveChartFilePath = args[4];

            width = Integer.parseInt(args[5]);
            height = Integer.parseInt(args[6]);

            isAverage = Boolean.parseBoolean(args[7]);
            isSaveImageIndividual = Boolean.parseBoolean(args[8]);

        } else {
            hostInfoFile = "/home/david/Execute/ResultProcess/config/HostInfo.conf";
            logFileName = "IndexingTest03";
            saveDirPath = "/home/david/Execute/ResultProcess/save/";
            remoteLogDirPath = "/home/search/elasticsearch_client/logs/";
            saveChartFilePath = "/home/david/";
            width = 2000;
            height = 500;

            isAverage = true;
            isSaveImageIndividual = false;
        }

        try {

            ResultProcess process = new ResultProcess(hostInfoFile, logFileName, saveDirPath, remoteLogDirPath, isAverage);
            if(isSaveImageIndividual) process.processIndividual(saveChartFilePath, width, height);
            else process.process(saveChartFilePath, width, height);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

