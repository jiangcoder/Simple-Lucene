package com.jiangcoder.bio;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class FirstServer {

    //①：accept 是阻塞的
    //②：单线程情况下，IO进行操作时，或写，或读，也是阻塞的
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(8000);
        System.out.println("服务端启动成功，监听端口为8000");
        while (true) {
            Socket socket = serverSocket.accept();
            InputStream inputStream = socket.getInputStream();
            byte[] buffer = new byte[1024];
            int len = 0;
            while ((len = inputStream.read(buffer)) > 0) {
                System.out.println(new String(buffer, 0, len));
            }
            //面向客户端写数据
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write("hello everybody!".getBytes());
        }
    }

}
