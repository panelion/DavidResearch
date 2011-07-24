package com.nexr.search.platform.parser;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: david
 * Date: 7/4/11
 * Time: 10:27 AM
 *
 * Xml 샘플 데이터 를 정상 적인 Xml 파일의 형태로 재 생성 해 낸다.
 */
public class XmlLogParser {


    private String _logSrc;
    private String _writeFileSrc;
    private BufferedReader _logReader;
    private BufferedWriter _logWriter;

    private int readCount = 0;
    private int writeCount = 0;



    public XmlLogParser() {


    }

    public void setLogSrc(String logSrc){
        this._logSrc = logSrc;
    }
    public void setWriteFileSrc(String writeFileName){
        this._writeFileSrc = writeFileName;
    }

    private String getReadLine() throws IOException {
        return _logReader.readLine();
    }

    public void parse() throws IOException {


        // Parsing LogData. If Log Data is not correct, throws the Trash.
        // Use Regular Express.

        String line;
        String strReg = "<TransactionLog>.*?</TransactionLog>";
        Pattern pattern = Pattern.compile(strReg);
        Matcher matches;

        File logFile = new File(_logSrc);
        _logReader = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), "UTF-8"));

        while((line = this.getReadLine()) != null) {

            line = line.trim();
            matches = pattern.matcher(line);

            if(!matches.matches()) {
                String tmpLine = line;
                boolean tmpWhile = true;

                while(tmpWhile){
                    tmpLine += this.getReadLine();
                    tmpLine = tmpLine.replaceAll("\n", "");
                    tmpLine = tmpLine.replaceAll("  ", "");
                    tmpLine = tmpLine.trim();

                    if(tmpLine.endsWith("</TransactionLog>")){
                        line = tmpLine;
                        tmpWhile = false;
                    }
                }

                readCount++;

                matches = pattern.matcher(line);

                if(!matches.matches()) {
                    System.out.println(line);
                    System.out.println("------------------------------------------------------------------------------------");
                } else {
                  this.writeLog(matches.group());
                }

            } else {
                readCount++;
                this.writeLog(matches.group());
            }
        }
    }

    private void writeLog(String line) throws IOException {
        line = line.replaceAll("  ", "");
        _logWriter.write(line);
        _logWriter.newLine();

        writeCount++;

    }

    public void open() throws IOException {

        File writeFile = new File(_writeFileSrc);
        if(!writeFile.exists()){
            writeFile.createNewFile();
        } else {
            // writeFile.delete();
            // writeFile.createNewFile();
        }

        FileWriter fw = new FileWriter(writeFile);
        _logWriter = new BufferedWriter(fw);
    }

    public void close(){

        try {
            _logReader.close();
            _logWriter.close();

            System.out.println("read count : " + readCount);
            System.out.println("write count : " + writeCount);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args){

        XmlLogParser logParser = new XmlLogParser();

        try {
            String logSrc, writeSrc;
            int roofCount;

            if(args.length > 0){
                logSrc = args[0];
                writeSrc = args[1];
                roofCount = Integer.parseInt(args[2]);
            } else {
                logSrc = "/home/david/Data/SearchPlatform/SDP/sdpData.log";
                writeSrc = "/home/david/Data/SearchPlatform/SDP/sdpParseData.log";
                roofCount = 20;
            }


            logParser.setLogSrc(logSrc);
            logParser.setWriteFileSrc(writeSrc);
            logParser.open();

            logParser.writeLog("<root>");
            for(int i = 0; i < roofCount; i++) logParser.parse();
            logParser.writeLog("</root>");

        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            logParser.close();
        }
    }
}
