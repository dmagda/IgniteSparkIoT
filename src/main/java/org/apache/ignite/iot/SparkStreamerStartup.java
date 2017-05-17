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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.ignite.iot.model.Sensor;
import org.apache.ignite.iot.model.TempKey;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import scala.Tuple2;

/**
 *
 */
public class SparkStreamerStartup {
    /** */
    public static int SENSORS_CNT = 1000;

    /** */
    private static JavaStreamingContext streamingCxt;

    /** */
    private static JavaIgniteContext igniteCxt;

    /** */
    private static JavaIgniteRDD<Integer, Sensor> sensorsRdd;

    /** */
    private static JavaIgniteRDD<TempKey, Float> tempRdd;

    /**
     * @param args
     */
    public static void main(String[] args) throws InterruptedException {
        // Create a local StreamingContext with two working thread and batch interval of 500 milliseconds.
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("IgniteSparkIoT");

        // Spark context.
        streamingCxt = new JavaStreamingContext(conf, Durations.milliseconds(1000));

        // Adjust the logger to exclude the logs of no interest.
        Logger.getRootLogger().setLevel(Level.ERROR);
        Logger.getLogger("org.apache.ignite").setLevel(Level.INFO);

        // Creates Ignite context and connects to it as a client application.
        igniteCxt = new JavaIgniteContext(
            streamingCxt.sparkContext(), "config/ignite-config.xml", true);

        System.out.println(">>> Spark Streamer is up and running.");

        // Getting a reference to sensors cache via the shared RDD.
        sensorsRdd = igniteCxt.<Integer, Sensor>fromCache("SensorCache");

        // Getting a reference to temperature cache via the shared RDD.
        tempRdd = igniteCxt.<Integer, Sensor>fromCache("TemperatureCache");

        System.out.println(">>> Shared RDDs are instantiated.");

        // Pre-loading sensors data to the cluster.
        preloadSensorsData();

        // Initiate streaming from IoT to Apache Ignite via Spark Streaming.
        streamSensorMeasurements();
    }

    /**
     * Filling out sensors cache with sample data.
     */
    private static void preloadSensorsData() throws InterruptedException {
        // Generating sample data.
        Random rand = new Random();

        ArrayList<Tuple2<Integer, Sensor>> sensors = new ArrayList<>();

        for (int i = 0; i < SENSORS_CNT; i++) {
            double lat = (rand.nextDouble() * -180.0) + 90.0;
            double lon = (rand.nextDouble() * -360.0) + 180.0;

            Sensor sensor = new Sensor("sensor_" + i, lat, lon);

            Tuple2<Integer, Sensor> entry = new Tuple2<Integer, Sensor>(i, sensor);

            sensors.add(entry);
        }

        // Creating JavaPairRDD from the sample data.
        JavaPairRDD<Integer, Sensor> data = streamingCxt.sparkContext().parallelizePairs(sensors);

        // Storing data in the cluster via shared RDD API.
        sensorsRdd.savePairs(data);

        System.out.println(" >>> Sensors information has been loaded to the cluster: " + sensorsRdd.count());
    }

    /**
     *
     */
    private static void streamSensorMeasurements() throws InterruptedException {
        // Create a DStream that will connect to hostname:port, like localhost:9999
        JavaReceiverInputDStream<String> rawData = streamingCxt.socketTextStream("localhost", 9999);

        // Split each sample into a tuple that contains 'sensorId' and `temperature` data.
        JavaPairDStream<Integer, Float> samples = rawData.mapToPair(new PairFunction<String, Integer, Float>() {
            @Override public Tuple2<Integer, Float> call(String s) throws Exception {
                String[] res = s.split(" ");

                return new Tuple2<Integer, Float>(Integer.parseInt(res[0]), Float.parseFloat(res[1]));
            }
        });

        // Transform the sample to Ignite cache entry pairs.
        JavaPairDStream<TempKey, Float> igniteEntries = samples.mapToPair(
            new PairFunction<Tuple2<Integer, Float>, TempKey, Float>() {
            @Override public Tuple2<TempKey, Float> call(Tuple2<Integer, Float> tuple2) throws Exception {
                return new Tuple2<TempKey, Float>(new TempKey(tuple2._1(), new Date()), tuple2._2());
            }
        });


        // Performing additional required transformation according to the use case.
        // ....

        // Push data to Apache Ignite cluster.
        igniteEntries.foreachRDD(new VoidFunction<JavaPairRDD<TempKey, Float>>() {
            @Override public void call(JavaPairRDD<TempKey, Float> rdd) throws Exception {
                tempRdd.savePairs(rdd);
            }
        });

        streamingCxt.start();

        System.out.println(" >>> Streaming of sensors' samples is activated.");

        // Scheduling the timer to execute queries over the cluster.
        new Timer().schedule(new TimerTask() {
            @Override public void run() {
                System.out.println(" >>> Samples count:" + tempRdd.count());

                Dataset ds = tempRdd.sql("SELECT sensorId, count(*) From Temperature WHERE temp > 70 and temp < 100" +
                    " GROUP BY sensorId ORDER BY sensorID");

                ds.show();
            }
        }, 1000, 4000);

        streamingCxt.awaitTermination();
    }
}
