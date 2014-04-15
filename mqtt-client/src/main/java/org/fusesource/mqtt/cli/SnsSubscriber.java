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
import java.io.FileReader;
import java.nio.ByteOrder;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.text.SimpleDateFormat;

/**
 * <p>
 * </p>
 *
 * @author <a>Xu Chengyang</a>
 */
public class SnsSubscriber {
    protected Properties config = new Properties();
    
    //MQTT related fields
    protected final MQTT mqtt = new MQTT();
    protected QoS qos = QoS.EXACTLY_ONCE; //default QoS
    protected final ArrayList<Topic> topics = new ArrayList<Topic>();
    protected boolean debug;
    protected boolean showTopic;
    
    //project
    protected String project;
    
    //Source byte order
    protected ByteOrder srcByteOrder;
    
    //Data API
    protected String data_url;

    public static void displayHelpAndExit(int exitCode) {
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

    public static void stdout(Object x) {
        System.out.println(x);
    }
    public static void stderr(Object x) {
        System.err.println(x);
    }


    public static String shift(LinkedList<String> argl) {
        if(argl.isEmpty()) {
            stderr("Invalid usage: Missing argument");
            displayHelpAndExit(1);
        }
        return argl.removeFirst();
    }
    
    public void processArgs(String[] args) throws Exception {
        // Process the arguments, if any
        LinkedList<String> argl = new LinkedList<String>(Arrays.asList(args));
        while (!argl.isEmpty()) {
            try {
                String arg = argl.removeFirst();
                if ("--help".equals(arg)) {
                    displayHelpAndExit(0);
                } else if ("-v".equals(arg)) {
                    mqtt.setVersion(shift(argl));
                } else if ("-h".equals(arg)) {
                    mqtt.setHost(shift(argl));
                } else if ("-k".equals(arg)) {
                    mqtt.setKeepAlive(Short.parseShort(shift(argl)));
                } else if ("-c".equals(arg)) {
                    mqtt.setCleanSession(false);
                } else if ("-i".equals(arg)) {
                    mqtt.setClientId(shift(argl));
                } else if ("-u".equals(arg)) {
                    mqtt.setUserName(shift(argl));
                } else if ("-p".equals(arg)) {
                    mqtt.setPassword(shift(argl));
                } else if ("--will-topic".equals(arg)) {
                    mqtt.setWillTopic(shift(argl));
                } else if ("--will-payload".equals(arg)) {
                    mqtt.setWillMessage(shift(argl));
                } else if ("--will-qos".equals(arg)) {
                    int v = Integer.parseInt(shift(argl));
                    if( v > QoS.values().length ) {
                        stderr("Invalid qos value : " + v);
                        displayHelpAndExit(1);
                    }
                    mqtt.setWillQos(QoS.values()[v]);
                } else if ("--will-retain".equals(arg)) {
                    mqtt.setWillRetain(true);
                } else if ("-d".equals(arg)) {
                    debug = true;
                } else if ("-s".equals(arg)) {
                    showTopic = true;
                } else if ("-q".equals(arg)) {
                    int v = Integer.parseInt(shift(argl));
                    if( v > QoS.values().length ) {
                        stderr("Invalid qos value : " + v);
                        displayHelpAndExit(1);
                    }
                    qos = QoS.values()[v];
                } else if ("-t".equals(arg)) {
                    topics.add(new Topic(shift(argl), qos));
                } else {
                    stderr("Invalid usage: unknown option: " + arg);
                    displayHelpAndExit(1);
                }
            } catch (NumberFormatException e) {
                stderr("Invalid usage: argument not a number");
                displayHelpAndExit(1);
            }
        }
    }
    
    public void loadConfig(String filename) throws Exception {
        //read in Subscriber config file
        FileReader reader = null;
        try {
            reader = new FileReader(filename);
            config.load(reader);                
    	} catch (IOException ex) {
    		ex.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        //set attribute using config file, overwrite default
        if (config.containsKey("topic") && config.containsKey("QoS")) {
            Topic tp = new Topic(config.getProperty("topic"),
                QoS.values()[Integer.parseInt(config.getProperty("QoS"))]);
            topics.clear();
            topics.add(tp);
        }
        if (config.containsKey("project")) {
            project = config.getProperty("project");
        }
        if (config.containsKey("host")) {
            mqtt.setHost(config.getProperty("host"));
        }
        if (config.containsKey("debug")) {
            debug = Boolean.parseBoolean(config.getProperty("debug"));
        }
        if (config.containsKey("ByteOrder")) {
            if (config.getProperty("ByteOrder").equals("0")) {
                srcByteOrder = ByteOrder.LITTLE_ENDIAN;
            } else if (config.getProperty("ByteOrder").equals("1")) {
                srcByteOrder = ByteOrder.BIG_ENDIAN;
            } else {
                stderr("Source Endianness definition is invalid.");
                System.exit(1);
            }
        } else {
            stderr("Source Endianness is not defined.");
            System.exit(1);
        }        
        if (config.containsKey("DATA_URL")) {
            data_url = config.getProperty("DATA_URL");
        } else {
            stderr("API_URL is not defined.");
            System.exit(1);
        }
    }
    
    public static void main(String[] args) throws Exception {
        SnsSubscriber main = new SnsSubscriber();                        
        
        //xcy Set System property for ssl connection
        System.setProperty("javax.net.ssl.keyStore", "../conf/mqttclient.ks");
        System.setProperty("javax.net.ssl.trustStore", "../conf/mqttclient.ts");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        
        //load config from file
        main.loadConfig("../conf/NM_CpuLoad.config");
        
        // Process the arguments, if any
        main.processArgs(args);                
        
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
        
        connection.listener(new org.fusesource.mqtt.client.Listener() { //xcy set listener for connection
            
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
                    if (showTopic) {
                        stdout("");
                        stdout("Topic: " + topic);
                        body.writeTo(System.out);
                        stdout("");
                    } else {
                        body.writeTo(System.out);
                    }
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
         System.out.println("connection.resume(); Done"); //xcy
        
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
