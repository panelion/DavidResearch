package com.panelion.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * - 파일과 관련된 기능 모음. -
 * Readable , Writable 을 통하여, 읽기 기능, 쓰기 기능을 사용 할 수 있다.
 * David.Woo - 2011.07.29
 *
 */
public class FileUtils {

    private File _file;
    private BufferedReader _reader;
    private BufferedWriter _writer;

    public FileUtils(String filePath) throws IOException {
        _file = new File(filePath);
        this.mkDirs(_file.getParent());
        if(!_file.exists()) _file.createNewFile();
    }

    public void createNewFile() throws IOException {
        this.mkDirs(_file.getParent());
        if(_file.exists()) _file.delete();

        _file.createNewFile();
    }

    /**
     * 디렉 토리를 생성 한다.
     */
    public void mkDirs(String strDirPath) {
        File dirFile = new File(strDirPath);
        dirFile.mkdirs();
    }

    /**
     * 파일을 읽기 전용 으로 세팅 한다.
     * @param encoding      encoding name
     * @throws java.io.IOException  읽을 파일이 없을 경우 에러가 난다.
     */
    public void setReadable(String encoding) throws IOException {
        _reader = new BufferedReader(new InputStreamReader(new FileInputStream(_file), encoding));
    }

    /**
     * 파일을 Row 단위로 읽는다.
     * @return              String Row
     * @throws java.io.IOException  읽을 때 에러가 날 경우.
     */
    public String getReadLine() throws IOException {
        return _reader.readLine();
    }

    /**
     * 파일을 쓰기 모드로 세팅 한다.
     * @param continued         이어 쓰기 여부.
     * @throws java.io.IOException      IOException
     */
    public void setWritable(boolean continued) throws IOException {
         _writer = new BufferedWriter(new FileWriter(_file, continued));
    }

    /**
     * 내용을 파일에 기록 한다.
     * @param line          기록할 내용.
     * @throws java.io.IOException  쓰기시 에러가 났을 경우.
     */
    public void writeLine(String line) throws IOException {
        _writer.write(line);
        _writer.write(System.getProperty("line.separator"));
    }

    public void close() throws IOException {
        if(_writer != null) _writer.close();
        if(_reader != null) _reader.close();
        if(_file != null) _file = null;
    }

    public List<Map<String, Object>> getHorizonTableFile() {
        return this.getHorizonTableFile("UTF-8", "\t");
    }

    public List<Map<String, Object>> getHorizonTableFile(String strSplit) {
        return this.getHorizonTableFile("UTF-8", strSplit);
    }

    public List<Map<String, Object>> getHorizonTableFile(String encoding, String strSplit) {
        List<Map<String, Object>> tableData = new ArrayList<Map<String, Object>>();
        List<String> columnList = new ArrayList<String>();

        try {
            this.setReadable(encoding);

            String line;
            int lineCount = 0;

            while((line = this.getReadLine()) != null) {
                // line no.1 is Column Define.
                if(lineCount == 0) {
                    String[] cols = line.split(strSplit);
                    for(String col : cols) {
                        columnList.add(col.trim());
                    }
                // Data rows
                } else {
                    Map<String, Object> map = new HashMap<String, Object>();
                    String[] cols = line.split(strSplit, columnList.size());

                    for(int i = 0 ; i < columnList.size(); i++) {
                        map.put(columnList.get(i), (cols[i] == null) ? "" : cols[i].trim());
                    }

                    tableData.add(map);
                }

                lineCount++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return tableData;
    }



    public static void main(String[] args) throws IOException {
        FileUtils fileUtils = new FileUtils("/home/david/test/test/test/1.txt");
        // fileUtils.mkDirs();
    }

}
