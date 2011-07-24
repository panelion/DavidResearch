package com.nexr.platform.search.result.utils;

import com.sshtools.j2ssh.SshClient;
import com.sshtools.j2ssh.authentication.AuthenticationProtocolState;
import com.sshtools.j2ssh.authentication.PasswordAuthenticationClient;
import com.sshtools.j2ssh.configuration.ConfigurationLoader;
import com.sshtools.j2ssh.configuration.SshConnectionProperties;
import com.sshtools.j2ssh.session.SessionChannelClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SshCommand {

    private Log log = LogFactory.getLog(this.getClass());
    private String _hostIp, _id, _password;
    private int _socketTimeOut;
    private String _promptString;

    private SshClient _sshClient;
    private SessionChannelClient _session;

    public SshClient getSshClient() {
        return _sshClient;
    }

    public String getPromptString() {
        return _promptString;
    }

    public SshCommand(String hostIp, String id, String password, String promptString) {
        _hostIp = hostIp;
        _id = id;
        _password = password;
        _socketTimeOut = 20000;
        _promptString = promptString;
    }


    public boolean connect() throws IOException {

        _sshClient = new SshClient();

        ConfigurationLoader.initialize(false);

        _sshClient.setSocketTimeout(_socketTimeOut);

        SshConnectionProperties properties = new SshConnectionProperties();

        properties.setHost(_hostIp);
        properties.setPort(22);
        // properties.setPrefPublicKey("ssh-dss");

        _sshClient.connect(properties, new AlwaysAllowingConsoleKnownHostsKeyVerification());

        PasswordAuthenticationClient pwd = new PasswordAuthenticationClient();

        pwd.setUsername(_id);
        pwd.setPassword(_password);

        int result = _sshClient.authenticate(pwd);

        if(result == AuthenticationProtocolState.COMPLETE ) {
            _session = _sshClient.openSessionChannel();
            _session.requestPseudoTerminal("gogrid", 200, 100, 0, 0, "");
            return true;
        } else {
            return false;
        }
    }

    public String executeCommand(String command) {

        StringBuffer returnValue = null;
		boolean promptReturned = false;
		byte[] buffer;
		OutputStream out;
		InputStream in;
		int read;
		String response;
		int i = 0;

		try {
            if (_session == null) {
                log.error("Session is not connected!");
                throw new Exception("Session is not connected!");
            }

            if(_session.startShell()) {
                out = _session.getOutputStream();
                out.write(command.getBytes());

                in = _session.getInputStream();

                buffer = new byte[1040];
                returnValue = new StringBuffer();

                while(promptReturned == false && (read = in.read(buffer)) > 0) {
                    response = new String(buffer, 0, read);
                    if (!response.isEmpty() && response.indexOf(_promptString) > 0) {
                        ++i;
                        if (i >= 2) {
                            promptReturned = true;
                        }
                    }
                    if (i == 1) returnValue.append(response);
                }
            }

		} catch (Exception e) {
	    	e.printStackTrace();
	    	log.error(e);
		}

		return returnValue.toString();

    }

    public void close(){

        _session.isClosed();
        _sshClient.disconnect();
        _sshClient = null;
    }

    public static void main(String args[]) throws IOException {

        SshCommand sshCommand = new SshCommand("10.1.8.1", "search", "Elastic^Search", "search@wolf201");


        if(sshCommand.connect()){
            String strCmd = "ls /home/search/\n";
            String result = sshCommand.executeCommand(strCmd);

            String[] dirs = result.split("  ");

            System.out.println("-----------------------------------------");

            for(String dir : dirs){
                if(!dir.trim().isEmpty()) System.out.println(dir);
            }

            sshCommand.close();
        }

    }
}
