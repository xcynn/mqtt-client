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

import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.ByteArrayOutputStream;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.mqtt.client.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * </p>
 *
 * @author <a>Xu Chengyang</a>
 */
public class PublisherMobileNodeDemo {
    private int itr = 1; //xcy
    private int loop = 0;
    private int datatime = 0;
    private final MQTT mqtt = new MQTT();
    private QoS qos = QoS.AT_MOST_ONCE;
    private UTF8Buffer topic;
    private Buffer body;
    private boolean debug;
    private boolean retain;
    private long count = 1;
    private long sleep;
    private boolean prefixCounter;

    private static void displayHelpAndExit(int exitCode) {
        stdout("");
        stdout("This is a simple mqtt client that will publish to a topic.");
        stdout("");
        stdout("Arguments: [-h host] [-k keepalive] [-c] [-i id] [-u username [-p password]]");
        stdout("           [--will-topic topic [--will-payload payload] [--will-qos qos] [--will-retain]]");
        stdout("           [-d] [-n count] [-s sleep] [-q qos] [-r] -t topic ( -pc | -m message | -z | -f file )");
        stdout("");
        stdout("");
        stdout(" -h : mqtt host uri to connect to. Defaults to tcp://localhost:1883.");
        stdout(" -k : keep alive in seconds for this client. Defaults to 60.");
        stdout(" -c : disable 'clean session'.");
        stdout(" -i : id to use for this client. Defaults to a random id.");
        stdout(" -u : provide a username (requires MQTT 3.1 broker)");
        stdout(" -p : provide a password (requires MQTT 3.1 broker)");
        stdout(" --will-topic : the topic on which to publish the client Will.");
        stdout(" --will-payload : payload for the client Will, which is sent by the broker in case of");
        stdout("                  unexpected disconnection. If not given and will-topic is set, a zero");
        stdout("                  length message will be sent.");
        stdout(" --will-qos : QoS level for the client Will.");
        stdout(" --will-retain : if given, make the client Will retained.");
        stdout(" -d : display debug info on stderr");
        stdout(" -n : the number of times to publish the message");
        stdout(" -s : the number of milliseconds to sleep between publish operations (defaut: 0)");
        stdout(" -q : quality of service level to use for the publish. Defaults to 0.");
        stdout(" -r : message should be retained.");
        stdout(" -t : mqtt topic to publish to.");
        stdout(" -itr : iteration of sending session."); //xcy
        stdout(" -m : message payload to send.");
        stdout(" -z : send a null (zero length) message.");
        stdout(" -f : send the contents of a file as the message.");
        stdout(" -pc : prefix a message counter to the message");
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
        PublisherMobileNodeDemo main = new PublisherMobileNodeDemo();
        
        //xcy Set System property for ssl connection
        System.setProperty("javax.net.ssl.keyStore", "../conf/mqttclient.ks");
        System.setProperty("javax.net.ssl.trustStore", "../conf/mqttclient.ts");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        // Process the arguments
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
                } else if ("-n".equals(arg)) {
                    main.count =  Long.parseLong(shift(argl));
                } else if ("-s".equals(arg)) {
                    main.sleep =  Long.parseLong(shift(argl));
                } else if ("-q".equals(arg)) {
                    int v = Integer.parseInt(shift(argl));
                    stdout("qv = "+v); //xcy
                    if( v > QoS.values().length ) {
                        stderr("Invalid qos value : " + v);
                        displayHelpAndExit(1);
                    }
                    main.qos = QoS.values()[v];
                    stdout("qos = "+main.qos); //xcy
                } else if ("-r".equals(arg)) {
                    main.retain = true;
                } else if ("-t".equals(arg)) {
                    main.topic = new UTF8Buffer(shift(argl));
                } else if ("-itr".equals(arg)) { //xcy
                    main.itr = Integer.parseInt(shift(argl)); //xcy
                } else if ("-m".equals(arg)) {
                    main.body = new UTF8Buffer(shift(argl)+"\n"); //xcy body is isntance of Buffer
                } else if ("-z".equals(arg)) {
                    main.body = new UTF8Buffer("");
                } else if ("-f".equals(arg)) {
                    File file = new File(shift(argl));
                    RandomAccessFile raf = new RandomAccessFile(file, "r");
                    try {
                        byte data[] = new byte[(int) raf.length()]; //xcy create byte array
                        raf.seek(0);
                        raf.readFully(data); //xcy read file into the byte array
                        main.body = new Buffer(data); //xcy wrapped to Buffer class (hawtbuf)
                    } finally {
                        raf.close();
                    }
                } else if ("-pc".equals(arg)) {
                    main.prefixCounter = true;
                } else {
                    stderr("Invalid usage: unknown option: " + arg);
                    displayHelpAndExit(1);
                }
            } catch (NumberFormatException e) {
                stderr("Invalid usage: argument not a number");
                displayHelpAndExit(1);
            }
        }

        if (main.topic == null) {
            stderr("Invalid usage: no topic specified.");
            displayHelpAndExit(1);
        }
        if (main.body == null) {
            stderr("Invalid usage: -z -m or -f must be specified.");
            displayHelpAndExit(1);
        }
        
        while(main.loop++<main.itr){ //xcy
        stdout("iteration "+main.loop+":"); //xcy
        //stdout("Let's execute"); //xcy
        main.execute(); //xcy simple while loop around main.execute() will not work
                        //xcy CountDownLatch() in .execute will be invoked, and 
                        //xcy program will shutdown. Need to delete the System.exit(0).
        stdout("Byebye"); //xcy this line will not be reached.
        }
        System.exit(0);
    }

    private void execute() {
        
        final CallbackConnection connection = mqtt.callbackConnection(); //xcy an instance of CallbackConnection

        final CountDownLatch done = new CountDownLatch(1); //work with done.await()
        
        // Handle a Ctrl-C event cleanly.
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                setName("MQTT client shutdown");
                connection.getDispatchQueue().execute(new Task() { 
                    //xcy anonymous class Task(), passed in for execute()
                    public void run() {
                        connection.disconnect(new Callback<Void>() {
                            //xcy pass in an instance of Callback
                            //xcy Callback() is an interface, implementation defined below
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
        
        connection.listener(new org.fusesource.mqtt.client.Listener() //xcy set listener
        {
            //xcy Passed in an instance implementing interface of .client.Listener
            //xcy This method set as the listener for the current connection.
            //xcy Interface .client.Listener(), implemented as below
            public void onConnected() {
                if (debug) {
                    stderr("ConnectedP"); //xcy
                }
            }

            public void onDisconnected() {
                if (debug) {
                    stderr("DisconnectedP"); //xcy
                }
            }

            public void onPublish(UTF8Buffer topic, Buffer body, Runnable ack) {
            //xcy empty for Publisher.
            //xcy listener/subscriber is using this field for received data operation
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
        connection.connect(new Callback<Void>() //xcy setup transport
        {
            //xcy Passed in an instance implementing interface of Callback
            //xcy This method take the Callback instance and create a Transport instance
            //xcy Callback is an interface impletement onFailure and onSuccess.
            public void onFailure(Throwable value) {
                if (debug) {
                    value.printStackTrace();
                } else {
                    stderr(value);
                }
                System.exit(2);
            }
            public void onSuccess(Void value) {
            stdout("connection.connect(new Callback<Void>(){onSuccess();}"); //xcy
            }
        });
        
      //while(itr-- > 0){   //xcy while loop here no use, Disconnection is done after the # of sent
                            //xcy unless recreate connection
        //xcy Execute new Task().run();
        new Task() 
        {
            //xcy Task implement Runnable, able to be executed by a thread.
            //xcy Task class is defined below
            
            long sent = 0;
            
            public void run() {
                final Task publish = this; //xcy the Task instance assigned to publish, is Runnable
                
                //xcy-------------------
                stdout("sent = "+sent); //xcy
                
                int nodeID = 11;
                SnsDemoAid mobileNode = new SnsDemoAid(true, nodeID); //xcy true for mobile
                
                if(sent%2 == 1) {
                    mobileNode.genLocData(datatime);
                    if(loop%2 == 1) {mobileNode.incCounter();}
                } else if (loop%2 == 0) {
                    mobileNode.genSensorData(0);
                    datatime = mobileNode.getTime();
                } else {
                    mobileNode.genSensorData(1);
                    datatime = mobileNode.getTime();
                }
                                
                //Buffer message  = body; //xcy THE body!!! Buffer class.
                //xcy Buffer class has constructor on byte[]. Can use that.
                Buffer message = new Buffer(mobileNode.getMessage()); //xcy
                topic = Buffer.utf8(mobileNode.getTopic()); //xcy
                stdout("Topic: "+mobileNode.getTopic());
                stdout("Message raw: "+mobileNode.getMessage());
                                
                if(prefixCounter) {
                    long id = sent + 1;
                    ByteArrayOutputStream os = new ByteArrayOutputStream(message.length + 15);
                    os.write(new AsciiBuffer(Long.toString(id)));
                    os.write(':');
                    os.write(body);
                    message = os.toBuffer();
                }
                
                //xcy To publish: topic is either UTF8Buffer or string class. 
                //xcy Default is UTF8Buffer, String will be converted in the background.
                //xcy publish method defined in CallbackConnection.
                
                stdout("qos = "+qos); //xcy
                connection.publish(topic, message, qos, retain, new Callback<Void>() {
                    public void onSuccess(Void value) {
                        sent ++;
                        if(debug) {
                            stdout("Sent message #"+sent);
                            stdout(""); //xcy
                        }
                        if( sent < count ) {
                            //xcy To repeat sending message with same topic, by re-execute publish = run()
                            //xcy no new class task instance is created.
                            
                            if(sleep>0) {
                                System.out.println("Sleeping");
                                connection.getDispatchQueue().executeAfter(sleep, TimeUnit.MILLISECONDS, publish);
                            } else {
                                connection.getDispatchQueue().execute(publish);
                            }
                        } else {
                            connection.disconnect(new Callback<Void>() {
                                public void onSuccess(Void value) {
                                    done.countDown();
                                }
                                public void onFailure(Throwable value) {
                                    done.countDown();
                                }
                            });                            
                        }
                    }
                    public void onFailure(Throwable value) {
                        stderr("Publish failed: " + value);
                        if(debug) {
                            value.printStackTrace();
                        }
                        System.exit(2);
                    }
                });
            }
        }.run(); //xcy run() method of new Task().
        try {
            done.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        /* delay 5s for while loop - xcy */
        try {
            stdout("sleep(5000) for next modality");
            Thread.sleep(5000);
        } catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        /* delay 5s for while loop - xcy */
        
//        System.exit(0);   //xcy system will shutdown when reaching this
                            //xcy comment out to enable looping at outer level.
    }
}
