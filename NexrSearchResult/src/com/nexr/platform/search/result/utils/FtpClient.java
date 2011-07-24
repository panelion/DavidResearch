package com.nexr.platform.search.result.utils;

import com.sshtools.j2ssh.SftpClient;
import com.sshtools.j2ssh.SshClient;
import com.sshtools.j2ssh.authentication.AuthenticationProtocolState;
import com.sshtools.j2ssh.authentication.PasswordAuthenticationClient;
import com.sshtools.j2ssh.configuration.ConfigurationLoader;
import com.sshtools.j2ssh.configuration.SshConnectionProperties;

import javax.swing.text.html.parser.Entity;
import java.io.IOException;

public class FtpClient {

    private SshClient _sshClient;
    private SftpClient _sFtpClient;

    public FtpClient(String hostName, String id, String pw) throws Exception {
        _sshClient = new SshClient();

        ConfigurationLoader.initialize(false);

        _sshClient.setSocketTimeout(20000);

        SshConnectionProperties properties = new SshConnectionProperties();

        properties.setHost(hostName);
        properties.setPort(22);

        _sshClient.connect(properties, new AlwaysAllowingConsoleKnownHostsKeyVerification());

        PasswordAuthenticationClient _auth = new PasswordAuthenticationClient();
        _auth.setUsername(id);
        _auth.setPassword(pw);

        int result = _sshClient.authenticate(_auth);

        if(result != AuthenticationProtocolState.COMPLETE){
            throw new Exception("Can't not login to Server");
        }

        _sFtpClient = _sshClient.openSftpClient();
    }

    /**
     * File 을 Upload 한다.
     * @param filePath
     * @return
     * @throws IOException
     */
    public boolean put(String filePath) throws IOException {
        boolean rtnVal = false;

        if(_sFtpClient != null){
            _sFtpClient.put(filePath);
            rtnVal = true;
        }

        return rtnVal;
    }

    /**
     * directory 경로를 변경 한다.
     * @param dirPath
     * @return
     */
    public boolean cd(String dirPath) throws IOException {
        boolean rtnVal = false;

        if(_sFtpClient != null) {
            _sFtpClient.cd(dirPath);
            rtnVal = true;
        }

        return rtnVal;
    }

    /**
     * local directory 경로를 변경 한다.
     * @param dirPath
     * @return
     */
    public boolean localCd(String dirPath) throws IOException {
        boolean rtnVal = false;
        if(_sFtpClient != null) {
            _sFtpClient.lcd(dirPath);
            rtnVal = true;
        }

        return rtnVal;
    }

    /**
     * 파일을 download 한다.
     * @param filePath          download 파일 경로
     * @param localFilePath     download 받을 로컬 파일 경로
     * @return
     */
    public boolean get(String filePath, String localFilePath) throws IOException {
        boolean rtnVal = false;
        if(_sFtpClient != null) {
            _sFtpClient.get(filePath, localFilePath);
            rtnVal = true;
        }
        return rtnVal;
    }

    /**
     * 파일 또는 directory 를 제거 한다.
     * @param fileOrDirPath 파일 또는 directory 경로
     * @return
     * @throws IOException
     */
    public boolean remove(String fileOrDirPath) throws IOException {
        boolean rtnVal = false;
        if(_sFtpClient != null) {
            _sFtpClient.rm(fileOrDirPath);
            rtnVal = true;
        }
        return rtnVal;
    }

    /**
     * 원격 서버에 있는 디렉토리를 로컬로 복사한다.
     * 만약 로컬에 디렉토리가 없는 경우 자동 생성된다.
     * @param remoteDirPath 원격 디렉토리 경로
     * @param localFilePath 로컬 디렉토리 경로
     * @return
     * @throws IOException
     */
    public boolean copyDir(String remoteDirPath, String localFilePath) throws IOException {
        boolean rtnVal = false;
        if(_sFtpClient != null) {
            _sFtpClient.copyRemoteDirectory(remoteDirPath, localFilePath, true, true, true, null);
            rtnVal = true;
        }
        return rtnVal;
    }

    /**
     * Connection 상태를 확인 한다.
     * @return
     * @throws IOException
     */
    public boolean isClosed() throws IOException {
        boolean rtnVal = false;
        if(_sFtpClient != null) {
            _sFtpClient.isClosed();
            rtnVal = true;
        }
        return rtnVal;
    }

    public boolean closed() throws IOException {
        boolean rtnVal = false;
        if(_sFtpClient != null) {
            _sFtpClient.quit();
            _sshClient.disconnect();

            _sFtpClient = null;
            _sshClient = null;

            rtnVal = true;
        }
        return rtnVal;
    }

    public static void main(String[] args){
        String hostName = "143.248.160.101";
        String id = "hadoop";
        String password = "hadooppw";

        try {
            FtpClient ftpClient = new FtpClient(hostName, id, password);
            ftpClient.copyDir("/home/search/sh/", "/home/david/sh/");

            ftpClient.closed();

        } catch(Exception e){
            e.printStackTrace();
        }

    }


}
