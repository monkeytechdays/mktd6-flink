package mktd6.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

public class LaunchHelper {

    private static final Logger LOG = LoggerFactory.getLogger(LaunchHelper.class);

    public static String getLocalIp() {
        try {
            Enumeration<NetworkInterface> n = NetworkInterface.getNetworkInterfaces();
            for (; n.hasMoreElements(); ) {
                NetworkInterface e = n.nextElement();
                Enumeration<InetAddress> a = e.getInetAddresses();
                for (; a.hasMoreElements(); ) {
                    InetAddress addr = a.nextElement();
                    String hostAddress = addr.getHostAddress();
                    if (hostAddress.startsWith("192.")) {
                        return hostAddress;
                    }
                }
            }
        }
        catch(Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return "localhost";
    }

}
