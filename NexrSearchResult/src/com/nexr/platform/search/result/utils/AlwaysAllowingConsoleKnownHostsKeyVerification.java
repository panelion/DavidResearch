package com.nexr.platform.search.result.utils;

import com.sshtools.j2ssh.transport.ConsoleKnownHostsKeyVerification;
import com.sshtools.j2ssh.transport.InvalidHostFileException;
import com.sshtools.j2ssh.transport.publickey.SshPublicKey;

public class AlwaysAllowingConsoleKnownHostsKeyVerification extends ConsoleKnownHostsKeyVerification {
    public AlwaysAllowingConsoleKnownHostsKeyVerification()
            throws InvalidHostFileException {
        super();
        // Don't not do anything else
    }
    @Override
    public void onHostKeyMismatch(String s, SshPublicKey sshpublickey, SshPublicKey sshPublicKey) {
        try
        {
            System.out.println("The host key supplied by " + s + " is: " + sshPublicKey.getFingerprint());
            System.out.println("The current allowed key for " + s + " is: " + sshpublickey.getFingerprint());
            System.out.println("~~~Using Custom Key verification, allowing to pass through~~~");

            allowHost(s, sshpublickey, false);
        }
        catch(Exception exception)
        {
            exception.printStackTrace();
        }
    }
    @Override
    public void onUnknownHost(String s, SshPublicKey sshpublickey) {
        try
        {
            System.out.println("The host " + s + " is currently unknown to the system");
            System.out.println("The host key fingerprint is: " + sshpublickey.getFingerprint());
            System.out.println("~~~Using Custom Key verification, allowing to pass through~~~");
            allowHost(s, sshpublickey, false);
        }
        catch(Exception exception)
        {
            exception.printStackTrace();
        }
    }
}
