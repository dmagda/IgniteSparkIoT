/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.iot;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Random;

/**
 *
 */
public class SensorDataGenerator {
    /**
     * @param args
     * @throws IOException
     */
    public static void main(String args[]) throws IOException {
        ServerSocket serverSocket = new ServerSocket(9999);

        System.out.println(" >>> Sensors' samples generator is up and running");

        Socket socket = serverSocket.accept();

        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

        Random rand = new Random();

        System.out.println(" >>> Connected to Spark. Start streaming data...");

        while (true) {
            int sensorId = rand.nextInt(SparkStreamerStartup.SENSORS_CNT);
            float temp = rand.nextInt(110);

            dos.write(String.valueOf(sensorId + " " + temp + "\n").getBytes("ASCII"));

            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            dos.flush();
        }
    }
}
