/**
 * Copyright (C) 2010-2012, FuseSource Corp.  All rights reserved.
 *
 *     http://fusesource.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fusesource.mqtt.cli;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.mqtt.client.*;

import java.io.IOException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.Date;
import java.text.SimpleDateFormat;

/**
 * <p>
 * </p>
 *
 * @author <a>Xu Chengyang</a>
 */
public class SubscriberSnsDemo {

    private final MQTT mqtt = new MQTT();
    private final ArrayList<Topic> topics = new ArrayList<Topic>();
    private boolean debug;
    private boolean showTopic;

    private static void displayHelpAndExit(int exitCode) {
        stdout("");
        stdout("This is a simple mqtt client that will subscribe to topics and print all messages it receives.");
        stdout("");
        stdout("Arguments: [-h host] [-k keepalive] [-c] [-i id] [-u username [-p password]]");
        stdout("           [--will-topic topic [--will-payload payload] [--will-qos qos] [--will-retain]]");
        stdout("           [-d] [-s]");
        stdout("           ( [-q qos] -t topic )+");
        stdout("");
        stdout("");
        stdout(" -h : mqtt host uri to connect to. Defaults to tcp://localhost:1883.");
        stdout(" -k : keep alive in seconds for this client. Defaults to 60.");
        stdout(" -c : disable 'clean session' (store subscription and pending messages when client disconnects).");
        stdout(" -i : id to use for this client. Defaults to a random id.");
        stdout(" -u : provide a username (requires MQTT 3.1 broker)");
        stdout(" -p : provide a password (requires MQTT 3.1 broker)");
        stdout(" --will-topic : the topic on which to publish the client Will.");
        stdout(" --will-payload : payload for the client Will, which is sent by the broker in case of");
        stdout("                  unexpected disconnection. If not given and will-topic is set, a zero");
        stdout("                  length message will be sent.");
        stdout(" --will-qos : QoS level for the client Will.");
        stdout(" --will-retain : if given, make the client Will retained.");
        stdout(" -d : dispaly debug info on stderr");
        stdout(" -s : show message topics in output");
        stdout(" -q : quality of service level to use for the subscription. Defaults to 0.");
        stdout(" -t : mqtt topic to subscribe to. May be repeated multiple times.");
        stdout("");
        System.exit(exitCode);
    }

    private static void stdout(Object x) {
        System.out.println(x);
    }
    private static void stderr(Object x) {
        System.err.println(x);
    }


    private static String shift(LinkedList<String> argl) {
        if(argl.isEmpty()) {
            stderr("Invalid usage: Missing argument");
            displayHelpAndExit(1);
        }
        return argl.removeFirst();
    }
    
    public static void main(String[] args) throws Exception {
        SubscriberSnsDemo main = new SubscriberSnsDemo();
        
        //xcy Set System property for ssl connection
        System.setProperty("javax.net.ssl.keyStore", "../conf/mqttclient.ks");
        System.setProperty("javax.net.ssl.trustStore", "../conf/mqttclient.ts");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        
        // Process the arguments
        QoS qos = QoS.AT_MOST_ONCE;
        LinkedList<String> argl = new LinkedList<String>(Arrays.asList(args));
        while (!argl.isEmpty()) {
            try {
                String arg = argl.removeFirst();
                if ("--help".equals(arg)) {
                    displayHelpAndExit(0);
                } else if ("-h".equals(arg)) {
                    main.mqtt.setHost(shift(argl));
                } else if ("-k".equals(arg)) {
                    main.mqtt.setKeepAlive(Short.parseShort(shift(argl)));
                } else if ("-c".equals(arg)) {
                    main.mqtt.setCleanSession(false);
                } else if ("-i".equals(arg)) {
                    main.mqtt.setClientId(shift(argl));
                } else if ("-u".equals(arg)) {
                    main.mqtt.setUserName(shift(argl));
                } else if ("-p".equals(arg)) {
                    main.mqtt.setPassword(shift(argl));
                } else if ("--will-topic".equals(arg)) {
                    main.mqtt.setWillTopic(shift(argl));
                } else if ("--will-payload".equals(arg)) {
                    main.mqtt.setWillMessage(shift(argl));
                } else if ("--will-qos".equals(arg)) {
                    int v = Integer.parseInt(shift(argl));
                    if( v > QoS.values().length ) {
                        stderr("Invalid qos value : " + v);
                        displayHelpAndExit(1);
                    }
                    main.mqtt.setWillQos(QoS.values()[v]);
                } else if ("--will-retain".equals(arg)) {
                    main.mqtt.setWillRetain(true);
                } else if ("-d".equals(arg)) {
                    main.debug = true;
                } else if ("-s".equals(arg)) {
                    main.showTopic = true;
                } else if ("-q".equals(arg)) {
                    int v = Integer.parseInt(shift(argl));
                    stdout("qv = "+v); //xcy
                    if( v > QoS.values().length ) {
                        stderr("Invalid qos value : " + v);
                        displayHelpAndExit(1);
                    }
                    qos = QoS.values()[v];
                    stdout("qos = "+qos); //xcy
                } else if ("-t".equals(arg)) {
                    main.topics.add(new Topic(shift(argl), qos));
                } else {
                    stderr("Invalid usage: unknown option: " + arg);
                    displayHelpAndExit(1);
                }
            } catch (NumberFormatException e) {
                stderr("Invalid usage: argument not a number");
                displayHelpAndExit(1);
            }
        }

        if (main.topics.isEmpty()) {
            stderr("Invalid usage: no topics specified.");
            displayHelpAndExit(1);
        }

        main.execute();
        System.exit(0);
    }

    private void execute() {
        final CallbackConnection connection = mqtt.callbackConnection();

        final CountDownLatch done = new CountDownLatch(1);
        
        // Handle a Ctrl-C event cleanly.
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                setName("MQTT client shutdown");
                if(debug) {
                    stderr("Disconnecting the client.");
                }
                connection.getDispatchQueue().execute(new Task() {
                    public void run() {
                        connection.disconnect(new Callback<Void>() {
                            public void onSuccess(Void value) {
                                done.countDown();
                            }
                            public void onFailure(Throwable value) {
                                done.countDown();
                            }
                        });
                    }
                });
            }
        });
        
        connection.listener(new org.fusesource.mqtt.client.Listener() 
        //xcy set listener for connection
        //xcy listener is the subscriber
        //xcy its methods are defined below
        {

            public void onConnected() {
                if (debug) {
                    stderr("Connected");
                }
            }

            public void onDisconnected() {
                if (debug) {
                    stderr("Disconnected");
                }
            }

            public void onPublish(UTF8Buffer topic, Buffer body, Runnable ack) {
                try {
                    /* //check byte array hexcode
                     * StringBuilder sb = new StringBuilder();
                     * for (byte b : body.toByteArray()) {sb.append(String.format("%02X ", b));}
                     * System.out.println(sb.toString());
                     */
                    
                    //xcy scan through topic string, look for nodeID and modalType
                    SnsTopic tp = new SnsTopic(topic.toString()); 

                    //xcy switch modalType call respective Data constructor
                    //xcy retrieve timestamp and dataValue
                    if (showTopic) { stdout("Topic: " + tp.getTopic());}
                    stdout("NodeID: " + tp.getNodeID());
                    //stdout("Raw data package:");
                    //body.writeTo(System.out);
                    //stdout("");
                    
                    stdout("Processed data package:");
                    
                    SimpleDateFormat ft = 
                            new SimpleDateFormat ("dd-MM-yyyy HH:mm:ss");
                    
                    //use the method for the relevant datatype to decode the monitoring data
                    
                    
                    while(true) {
                        if (tp.getDatatype()=="Light") {
                            LightData ld = new LightData(body.toByteArray());
                            stdout("Time = "+ft.format(new Date((long)ld.getTime()*1000)));
                            stdout("Light value = "+ld.getLight());
                            
                            PrintWriter outputStreamL = 
                                            new PrintWriter(new FileWriter("LightData.csv", true));
                            PrintWriter outputStreamData = 
                                            new PrintWriter(new FileWriter("SensorData.csv", true));
                            try {
                                    outputStreamL.println(tp.getNodeID()+","+
                                            "Light"+","+
                                            ft.format(new Date((long)ld.getTime()*1000))+","+
                                            ld.getLight());
                                    outputStreamData.println(tp.getNodeID()+","+
                                            "Light"+","+
                                            ft.format(new Date((long)ld.getTime()*1000))+","+
                                            ld.getLight());
                                } finally {
                                    if (outputStreamL != null) {
                                        outputStreamL.close();
                                    }
                                    if (outputStreamData != null) {
                                        outputStreamData.close();
                                    }
                            }
                            break;
                        }
                        
                        if (tp.getDatatype()=="Temperature") {
                            TempData td = new TempData(body.toByteArray());
                            stdout("Time = "+ft.format(new Date((long)td.getTime()*1000)));
                            stdout("Temperature = "+td.getTemp());
                            PrintWriter outputStreamT = 
                                            new PrintWriter(new FileWriter("TemperatureData.csv", true));
                            PrintWriter outputStreamData = 
                                            new PrintWriter(new FileWriter("SensorData.csv", true));
                            try {
                                    outputStreamT.println(tp.getNodeID()+","+
                                            "Temperature"+","+
                                            ft.format(new Date((long)td.getTime()*1000))+","+
                                            td.getTemp());
                                    outputStreamData.println(tp.getNodeID()+","+
                                            "Temperature"+","+
                                            ft.format(new Date((long)td.getTime()*1000))+","+
                                            td.getTemp());
                                } finally {
                                    if (outputStreamT != null) {
                                        outputStreamT.close();
                                    }
                                    if (outputStreamData != null) {
                                        outputStreamData.close();
                                    }
                            }
                            break;
                        }
                        if (tp.getDatatype()=="Location") {
                            LocationData locd = new LocationData(body.toByteArray());
                            stdout("Time = "+ft.format(new Date((long)locd.getTime()*1000)));
                            stdout("Latitude = "+locd.getLat());
                            stdout("Longitude = "+locd.getLong());
                            PrintWriter outputStreamLoc = 
                                            new PrintWriter(new FileWriter("LocationData.csv", true));
                            PrintWriter outputStreamData = 
                                            new PrintWriter(new FileWriter("SensorData.csv", true));
                            try {
                                    outputStreamLoc.println(tp.getNodeID()+","+
                                            "Location"+","+
                                            ft.format(new Date((long)locd.getTime()*1000))+","+
                                            locd.getLat()+","+
                                            locd.getLong());
                                    outputStreamData.println(tp.getNodeID()+","+
                                            "Location"+","+
                                            ft.format(new Date((long)locd.getTime()*1000))+","+
                                            locd.getLat()+","+
                                            locd.getLong());
                                } finally {
                                    if (outputStreamLoc != null) {
                                        outputStreamLoc.close();
                                    }
                                    if (outputStreamData != null) {
                                        outputStreamData.close();
                                    }
                            }
                            break;
                        }
                        System.out.println("Error invalid datatype.");
                        break;
                    }
                    stdout("");
                    
                    //xcy end
                    
//                    if (showTopic) {
//                        stdout("");
//                        stdout("Topic: " + topic);
//                        body.writeTo(System.out);
//                        stdout("");
//                    } else {
//                        body.writeTo(System.out); //xcy display body on output
//                    }
                    
                    ack.run();
                } catch (IOException e) {
                    onFailure(e);
                }
            }

            public void onFailure(Throwable value) {
                if (debug) {
                    value.printStackTrace();
                } else {
                    stderr(value);
                }
                System.exit(2);
            }
        });

        connection.resume(); //xcy AtomicInteger stuff
         System.out.println("Done connection.resume();"); //xcy
        
        //xcy setup transport in connection using .connect() method.
        //xcy an instance of Callback of Void type is passed in.
        //xcy contains two methods declaration
        connection.connect(new Callback<Void>() {
            public void onFailure(Throwable value) {
                if (debug) {
                    value.printStackTrace();
                } else {
                    stderr(value);
                }
                System.exit(2);
            }

            public void onSuccess(Void value) {
                final Topic[] ta = topics.toArray(new Topic[topics.size()]);
                connection.subscribe(ta, new Callback<byte[]>() {
                    public void onSuccess(byte[] value) {
                        if(debug) {
                            for (int i = 0; i < value.length; i++) {
                                stdout("value.length = " + value.length); //xcy
                                stderr("Subscribed to Topic: " + ta[i].name() + " with QoS: " + QoS.values()[value[i]]);
                            }
                        }                        
                    }
                    public void onFailure(Throwable value) {
                        stderr("Subscribe failed: " + value);
                        if(debug) {
                            value.printStackTrace();
                        }
                        System.exit(2);
                    }
                });
            }
        });

        try {
            done.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);

    }

}
