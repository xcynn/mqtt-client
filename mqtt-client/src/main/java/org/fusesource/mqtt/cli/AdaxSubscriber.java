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

import java.io.FileReader;
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Properties;

/**
 * <p>
 * </p>
 *
 * @author <a>Xu Chengyang</a>
 */
public class AdaxSubscriber {
    
    private final MQTT mqtt = new MQTT();
    private final ArrayList<Topic> topics = new ArrayList<Topic>();
    private boolean debug;
    private boolean showTopic;
        
    //Source byte order
    protected static ByteOrder srcByteOrder = ByteOrder.LITTLE_ENDIAN;

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
        stdout(" -v : MQTT version to use 3.1 or 3.1.1. (default: 3.1)");
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
        AdaxSubscriber main = new AdaxSubscriber();
        
        //set attribute using config file
        Properties config = new Properties();
        try (FileReader reader = new FileReader("./conf/adaxsub.config")) {
            config.load(reader);                
    	} catch (IOException ex) {
    		ex.printStackTrace();
        } 
        if (config.containsKey("topic") && config.containsKey("QoS")) {
            Topic tp = new Topic(config.getProperty("topic"),
                QoS.values()[Integer.parseInt(config.getProperty("QoS"))]);
            main.topics.clear();
            main.topics.add(tp);
        } else {
            System.out.println("topic or QoS is not defined.");
            System.exit(1);
        }
        if (config.containsKey("host")) {
            main.mqtt.setHost(config.getProperty("host"));
        } else {
            System.out.println("topic or QoS is not defined.");
            System.exit(1);
        }
        
        //Set System property for ssl connection
        System.setProperty("javax.net.ssl.keyStore", "./ssl/client.ks");
        System.setProperty("javax.net.ssl.trustStore", "./ssl/client.ts");
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
                } else if ("-v".equals(arg)) {
                    main.mqtt.setVersion(shift(argl));
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
                    if( v > QoS.values().length ) {
                        stderr("Invalid qos value : " + v);
                        displayHelpAndExit(1);
                    }
                    qos = QoS.values()[v]; 
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

        //display mqtt attribute
        stdout("Topic: " + main.topics.get(0).name()+"\tQoS: "+main.topics.get(0).qos());
        stdout("Host: " + main.mqtt.getHost());
        
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
        
        connection.listener(new org.fusesource.mqtt.client.Listener() {

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
                PrintWriter outputStreamData = null;
                try {
                    outputStreamData = new PrintWriter(new FileWriter("/usr/local/dataimport/SensorData.csv", true));
                    
                    if (showTopic) {
                        stdout("Topic: " + topic.toString());
                    }
                    
                    //xcy scan through topic string, look for nodeID
                    String[] topicFields = topic.toString().split("\\/", 0);
                    String nodeSite = topicFields[2];
                    String nodeID = topicFields[3];
                    String modality = topicFields[4];

                    //check byte array hexcode
                    StringBuilder sb = new StringBuilder();
                    for (byte b : body.toByteArray()) {sb.append(String.format("%02X ", b));}
                    System.out.println(sb.toString());
                    
                    long timestamp;
                    
                    //extract timestamp
                    ByteBuffer buf = ByteBuffer.wrap(body.toByteArray());
                    buf.order(srcByteOrder);
                    timestamp = buf.getInt() & 0xffffffffL;
                    
                    SimpleDateFormat ft =
                            new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
                    
                    switch (modality) {
                        case "BoxOpen":
                            if (buf.capacity() != 5) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+(int)(buf.getChar() & 0xffff));
                            break;
                        case "Temperature":
                            if (buf.capacity() != 8) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+buf.getFloat());
                            break;
                        case "Humidity":
                            if (buf.capacity() != 8) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+buf.getFloat());
                            break;
                        case "Noise": 
                            if (buf.capacity() != 8) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+buf.getFloat());
                            break;
                        case "PM2d5": 
                            if (buf.capacity() != 6) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+(int)(buf.getShort() & 0xffff));
                            break;
                        case "PM10": 
                            if (buf.capacity() != 6) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+(int)(buf.getShort() & 0xffff));
                            break;
                        case "Light": 
                            if (buf.capacity() != 8) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+buf.getFloat());
                            break;
                        case "SO2": 
                            if (buf.capacity() != 6) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+(int)(buf.getShort() & 0xffff));
                            break;
                        case "CO": 
                            if (buf.capacity() != 8) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+buf.getFloat());
                            break;
                        case "CO2": 
                            if (buf.capacity() != 8) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+buf.getFloat());
                            break;
                        case "NO2": 
                            if (buf.capacity() != 8) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+buf.getFloat());
                            break;
                        case "O3": 
                            if (buf.capacity() != 8) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+buf.getFloat());
                            break;
                        case "H2S_NH3_H2": 
                            if (buf.capacity() != 8) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+buf.getFloat());
                            break;
                        case "CH4_CO": 
                            if (buf.capacity() != 8) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+buf.getFloat());
                            break;
                        case "Location": 
                            if (buf.capacity() != 20) {
                                System.out.println("wrong data packet size for modality: " + modality); break;
                            }
                            outputStreamData.println(nodeID+","+modality+","+ft.format(new Date(timestamp*1000))+
                                    ","+","+buf.getDouble()+","+buf.getDouble());
                            break;
                        default:
                    }
                } catch (IOException e) {
                    onFailure(e);
                } finally {
                    ack.run();
                    if (outputStreamData != null) {
                        outputStreamData.close();
                }
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
