/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.fusesource.mqtt.cli;

/**
 * @author xcy
 */

import java.nio.ByteBuffer;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;

public class LightData {
    private int timestamp;
    private float lightVal;
    private static final int LIGHT_DATA_SIZE = 8;
    
    //Constructors
    public LightData(){
        timestamp = 0;
        lightVal = 0;
    }
    
    public LightData(float light){
        timestamp = (int)(new Date().getTime()/1000L);
        lightVal = light;
    }
    
    public LightData(int time, float light){
        timestamp = time;
        lightVal = light;
    }
    
    public LightData(byte [] byteArray) {
        readFromBytes(byteArray);
    }

    public LightData(String filename) throws IOException {
        readFromFile(filename);
    }
    
    //methods
    public float getLight (){
        return this.lightVal;
    }
    
    public int getTime(){
        return this.timestamp;
    }
    
    public void printLight(){
        System.out.println("Time = "+timestamp+"; lightValue = "+lightVal);        
    }
    
    public void writeToFile(String filename) throws IOException {
        byte[] byteArray = new byte[8];
        // ByteBuffer to be backed by the buffer byte array
        ByteBuffer timestampBuf = ByteBuffer.wrap(byteArray, 0, 4);
        ByteBuffer valueBuf = ByteBuffer.wrap(byteArray, 4, 4);
        
        timestampBuf.putInt(timestamp);
        valueBuf.putFloat(lightVal);
        //byte array buffer content changed after put method.
        
        FileOutputStream file = new FileOutputStream(filename);
        file.write(byteArray); //input must be byte[]. Not byteBuffer
        file.close();
    }
    
    public void readFromFile(String filename) throws IOException {
        byte[] byteArray = new byte[8];
        ByteBuffer timestampBuf = ByteBuffer.wrap(byteArray, 0, 4);
        ByteBuffer valueBuf = ByteBuffer.wrap(byteArray, 4, 4);
        
        FileInputStream file = new FileInputStream(filename);
        file.read(byteArray);
        file.close();
        
        timestamp = timestampBuf.getInt();
        lightVal = valueBuf.getFloat();
    }
    
    public byte[] writeToBytes() {
        byte[] ByteArray = new byte[8];
        // ByteBuffer to be backed by the buffer byte array
        ByteBuffer timestampBuf = ByteBuffer.wrap(ByteArray, 0, 4);
        ByteBuffer valueBuf = ByteBuffer.wrap(ByteArray, 4, 4);
        
        timestampBuf.putInt(timestamp);
        valueBuf.putFloat(lightVal);
        //byte array buffer content changed after put method.
        
        return ByteArray;
    }
    
    public void readFromBytes(byte [] byteArray) {
        if(byteArray.length!=LIGHT_DATA_SIZE){
            System.out.println("wrong data packet size. Size need to be "+
                    LIGHT_DATA_SIZE+" bytes.");
            return;
        } else{
            ByteBuffer timestampBuf = ByteBuffer.wrap(byteArray, 0, 4);
            ByteBuffer valueBuf = ByteBuffer.wrap(byteArray, 4, 4);
            timestamp = timestampBuf.getInt();
            lightVal = valueBuf.getFloat();
        }
    }

    
    public static void main(String[] args) throws Exception {
        
        LightData ld1 = new LightData();
        ld1.printLight();
        
        LightData ld2 = new LightData(1.5f);
        ld2.printLight();
        
        LightData ld3 = new LightData(10, 1.5f);
        ld3.printLight();
        
        byte[] msg1 = new byte[8];
        LightData ld4 = new LightData(msg1);
        ld4.printLight();
        
        LightData ld5 = new LightData(50, 8.5f);
        ld5.printLight();
        ld5.writeToFile("Light_testfile.dat");
        
        LightData ld6 = new LightData("Light_testfile.dat");
        ld6.printLight();
        
        LightData ld7 = new LightData(100, 8.5f);
        ld7.printLight();
        
        byte[] msg2 = new byte[8];
        msg2 = ld7.writeToBytes();
        LightData ld8 = new LightData(msg2);
        ld8.printLight();        
        
        System.out.println("timestamp = " + (ld8.getTime()) + "\tvalue = " + (ld8.getLight()) + "\n");
    }
}
