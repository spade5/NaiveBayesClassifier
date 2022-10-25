package org.spade5.Predition;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class Utils {

    private static Socket socket;
    private static OutputStream os = null;

    static {

        try {
            socket = new Socket("172.31.0.2", 8888);
            os = socket.getOutputStream();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 主机名
     * @return
     */
    public static String getHost(){

        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * PID进程号
     * @return
     */
    public static String getPID(){

        try {
            RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
            String pidAndHost = runtimeMXBean.getName();
            return pidAndHost.substring(0, pidAndHost.indexOf("@"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * TID线程号
     * @return
     */
    public static String getTID(){

        try {
            return Thread.currentThread().getName();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * TID线程号
     * @return
     */
    public static String getOID(Object o){

        try {
            return o.getClass().getName() + "@" + o.hashCode();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String prefix(Object o, String outInfo) {

        String outStr = "[" + getHost() + ":" + getPID() + ":" + getTID() + ":" + getOID(o) + "]:" + outInfo;
        return outStr;
    }

    public static void outInfo(String info) {

        try {
//            os.write((prefix(o, info) + "\r\n").getBytes());
            os.write((info + "\r\n").getBytes());
            os.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
