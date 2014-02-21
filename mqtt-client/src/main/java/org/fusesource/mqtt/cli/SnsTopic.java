/*
 * Copyright 2013 FuseSource, Corp..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.mqtt.cli;

/**
 *
 * @author xcy
 */
public class SnsTopic {
    static String[] topicHead = {"/sns/jld_site_1/","/sns/jld_mobile/"};
    static String[] dataTypes = {"Light","Temperature","Location"};
    
    private String site = "";
    private String nodeID = "";
    private String dataType;
    private String topic;
    
    public SnsTopic(String topic) {
        this.topic = topic;
        String[] strArray = topic.split("\\/",0);
        site = strArray[2];
        nodeID = strArray[3];
        
        if(strArray[4].equals("Light")) {
            dataType = "Light";
        } else if(strArray[4].equals("Temperature")) {
            dataType = "Temperature";
        } else if(strArray[4].equals("Location")) {
            dataType = "Location";
        } else System.out.println("Error. No valid data type found.");
        
//        for(String str: strArray) 
//        {
//        System.out.println(str);
//        }
    }
    
    public String getTopic() {
        return topic;
    }
    
    public String getSite() {
        return site;
    }
    
    public String getNodeID() {
        return nodeID;
    }
    
    public String getDatatype() {
        return dataType;
    }
    
    public static void main(String[] args) {
        new SnsTopic("sns/jasdf/123/asdf");
        new SnsTopic("/sns/jasdf/123/asdf");
    }
}
