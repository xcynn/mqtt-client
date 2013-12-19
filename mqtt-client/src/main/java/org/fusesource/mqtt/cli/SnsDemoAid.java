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
 * @author xcy
 */

public class SnsDemoAid {
    private static int counter = 0;
    private static String[] topicHeadArray = {"/sns/jld_site_1/","/sns/jld_mobile/"};
    private static String[] dataTypeArray = {"Light","Temperature","Location"};
    private static double[][] route = {
        {1.333023,103.740858},
        {1.332408,103.741279},
        {1.331854,103.741793},
        {1.331807,103.742877},
        {1.332551,103.743648},
        {1.333159,103.744079},
        {1.333721,103.744227},
        {1.334049,103.743972},
        {1.3346,103.743508},
        {1.335143,103.743274},
        {1.335683,103.742865},
        {1.336140,103.742588},
        {1.336713,103.741749},
        {1.336945,103.741279},
        {1.336885,103.740097},
        {1.336460,103.739516},
        {1.336155,103.73941},
        {1.335357,103.73909},
        {1.335501,103.739786},
        {1.334585,103.740571},
        {1.333849,103.740676}
    };
    
    boolean isMobile = false;
    int nodeID = 0;
    String topic = null;
    byte[] message = null;
    int timestamp = 0;
    
    //constructor for node property
    public SnsDemoAid() {
        isMobile = false;
        nodeID = 1;
    }
    
    public SnsDemoAid(boolean mobile, int node) {
        isMobile = mobile;
        nodeID = node;
    }
    
    //methods
    public void genSensorData (int dataType){
        //xcy dataType: 0 - light, 1 -temperature.

        int loc_index = isMobile?1:0;
        topic = topicHeadArray[loc_index] + Integer.toString(nodeID) + 
                "/" + dataTypeArray[dataType];
        if(dataType == 0) {
            LightData light = new LightData((float)(30000+3000*Math.random()));
            message = light.writeToBytes();
            timestamp = light.getTime();
        } else if (dataType == 1) {
            TempData temp = new TempData((float)(25+2*Math.random()));
            message = temp.writeToBytes();
            timestamp = temp.getTime();
        } else System.out.println("invalid dType. 0 - light, 1 -temperature.");
    }
    
    //Assume a location data will be generated at the same time of other sensor date
    //generate location with the known timestamp
    public void genLocData (int time){
        int loc_index = isMobile?1:0;
        if(!isMobile) {
            System.out.println("genLocData method invalid for non-mobile node.");
            return;
        }
        timestamp = time;                
        topic = topicHeadArray[loc_index] + Integer.toString(nodeID) + 
                "/" + dataTypeArray[2];
        message = new LocationData(time, route[counter%21][0], route[counter%21][1]).writeToBytes();
    }
    
    public void incCounter() {
        counter++;
    }
    
    public boolean getIsMobile() {
        return isMobile;
    }
    
    public int getNodeID() {
        return nodeID;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public byte[] getMessage() {
        return message;
    }
    
    public int getTime() {
        return timestamp;
    }
    
    public static void main (String[] args) {
        SnsDemoAid aid1 = new SnsDemoAid(false, 1);
        aid1.genSensorData(0);
        int i = 0;
        while (i++ < 100000) {
            aid1.genSensorData(1);
            
        }
       
        
    }
    
    
}
