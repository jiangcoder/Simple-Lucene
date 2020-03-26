package com.jiangcoder.bio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class FirstClient {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("127.0.0.1", 8000);
        OutputStream outputStream = socket.getOutputStream();
        outputStream.write("hello server! I am client".getBytes());
        outputStream.flush();;
        InputStream inputStream = socket.getInputStream();
        byte[] buffer = new byte[1024];
        int len = 0;
        while ((len = inputStream.read(buffer)) > 0) {
            System.out.println(new String(buffer, 0, len));
        }
    }
}
