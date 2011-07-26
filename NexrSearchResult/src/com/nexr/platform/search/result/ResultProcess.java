package com.nexr.platform.search.result;

import com.nexr.platform.search.result.entity.ServerInfoEntity;
import com.nexr.platform.search.result.utils.Chart;
import com.nexr.platform.search.result.utils.FtpClient;

import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.io.*;
import java.util.ArrayList;

/**
 * ElasticSearch Client Result Log 파일을 download 후, 분석 하여, chart image 로 저장 한다.
 * David.Woo - 2011.07.20
 *
 */
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

    /**
     * Constructor
     *
     * @param hostInfoFile      Tab 으로 구분된 Server 정보를 저장한 파일의 경로.
     * @param logFileName       서버에 저장된 로그 파일의 이름. 확장자 는 제외 한다.
     * @param saveDirPath       로컬에 이미지 및 로그 파일 이 저장 될 경로
     * @param remoteLogDirPath  서버 로그 파일이 저장된 directory 의 경로
     * @param isAverage         TPS 평균치 저장 시 True, Second 별 Chart 생성 시 False
     *
     * @throws IOException      Server 정보 파일을 읽다가 에러가 날 경우의 Exception
     */
    public ResultProcess(String hostInfoFile, String logFileName, String saveDirPath, String remoteLogDirPath, boolean isAverage) throws Exception {

        /**
         * download 할 서버 정보를 저장 한다.
         */
        serverList = new ArrayList<ServerInfoEntity>();

        File file = new File(hostInfoFile);
        if(!file.exists()) throw new Exception("can't find Host Info File.");
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

    /**
     * 서버별 로 로그 파일 download 후, Chart Data 를 생성 한다.
     * 생성 후, 챠트 image 를 저장 한다.
     *
     * @param saveFilePath  chart Image 저장 directory 경로
     * @param width 저장할 이미지 가로 사이즈
     * @param height 저장할 이미지 세로 사이즈
     */
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

    /**
     * 서버별 로 로그 파일 download 후, Chart Data 를 생성 한다.
     * 생성 후, 챠트 image 를 개별적 으로 저장 한다.
     *
     * @param saveFilePath  chart Image 저장 directory 경로
     * @param width 저장할 이미지 가로 사이즈
     * @param height 저장할 이미지 세로 사이즈
     */
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

    /**
     * log file 의 data 를 parsing 하여, Chart DataSet 생성.
     *
     * @param localFilePath log file 경로
     * @param entity 서버 정보 Entity Class
     * @return XySeries Data Set.
     */
    private XYSeries makeDataSet(String localFilePath, ServerInfoEntity entity) {

        String chartName = entity.getHostName();
        XYSeries series = new XYSeries(chartName);

        try {
            File file = new File(localFilePath);

            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), _ENCODING));

            String row;
            while((row = reader.readLine()) != null){
                if(!row.isEmpty()) {
                    double x, y;
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

    /**
     * TODO : 시간 단위 분 단위로 이미지 를 추출 해 낼 수 있게끔 수정 하기.
     * @param args
     */
    public static void main(String[] args){

        String hostInfoFile, logFileName, saveDirPath, remoteLogDirPath, saveChartFilePath;
        boolean isAverage;
        boolean isSaveImageIndividual;

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
            hostInfoFile = "/home/david/IdeaProjects/DavidResearch/NexrSearchResult/config/HostInfo.conf";
            logFileName = "SdpIndexingTest01";
            saveDirPath = "/home/david/IdeaProjects/DavidResearch/NexrSearchResult/saves/";
            remoteLogDirPath = "/home/search/elasticsearch_client/logs/";
            saveChartFilePath = "/home/david/";
            width = 2000;
            height = 500;

            isAverage = false;
            isSaveImageIndividual = true;
        }

        try {

            ResultProcess process = new ResultProcess(hostInfoFile, logFileName, saveDirPath, remoteLogDirPath, isAverage);
            if(isSaveImageIndividual) process.processIndividual(saveDirPath, width, height);
            else process.process(saveDirPath, width, height);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

