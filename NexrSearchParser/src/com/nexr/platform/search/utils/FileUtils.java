package com.nexr.platform.search.utils;

import java.io.*;

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
     * @throws IOException  읽을 파일이 없을 경우 에러가 난다.
     */
    public void setReadable(String encoding) throws IOException {
        _reader = new BufferedReader(new InputStreamReader(new FileInputStream(_file), encoding));
    }

    /**
     * 파일을 Row 단위로 읽는다.
     * @return              String Row
     * @throws IOException  읽을 때 에러가 날 경우.
     */
    public String getReadLine() throws IOException {
        return _reader.readLine();
    }

    /**
     * 파일을 쓰기 모드로 세팅 한다.
     * @param continued         이어 쓰기 여부.
     * @throws IOException      IOException
     */
    public void setWritable(boolean continued) throws IOException {
         _writer = new BufferedWriter(new FileWriter(_file, continued));
    }

    /**
     * 내용을 파일에 기록 한다.
     * @param line          기록할 내용.
     * @throws IOException  쓰기시 에러가 났을 경우.
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

    public static void main(String[] args) throws IOException {
        FileUtils fileUtils = new FileUtils("/home/david/test/test/test/1.txt");
        // fileUtils.mkDirs();
    }

}
