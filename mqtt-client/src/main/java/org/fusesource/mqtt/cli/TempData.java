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

public class TempData {
    private int timestamp;
    private float tempVal;
    private static final int TEMP_DATA_SIZE = 8;
    
    //Constructors
    public TempData(){
        timestamp = 0;
        tempVal = 0;
    }
    
    public TempData(float temp){
        timestamp = (int)(new Date().getTime()/1000L);
        tempVal = temp;
    }
    
    public TempData(int time, float temp){
        timestamp = time;
        tempVal = temp;
    }
    
    public TempData(byte [] byteArray) {
        readFromBytes(byteArray);
    }

    public TempData(String filename) throws IOException {
        readFromFile(filename);
    }
    
    //methods
    public float getTemp (){
        return this.tempVal;
    }
    
    public int getTime(){
        return this.timestamp;
    }
    
    public void printTemp(){
        System.out.println("Time = "+timestamp+"; tempValue = "+tempVal);        
    }
    
    public void writeToFile(String filename) throws IOException {
        byte[] byteArray = new byte[8];
        // ByteBuffer to be backed by the buffer byte array
        ByteBuffer timestampBuf = ByteBuffer.wrap(byteArray, 0, 4);
        ByteBuffer valueBuf = ByteBuffer.wrap(byteArray, 4, 4);
        
        timestampBuf.putInt(timestamp);
        valueBuf.putFloat(tempVal);
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
        tempVal = valueBuf.getFloat();
    }
    
    public byte[] writeToBytes() {
        byte[] ByteArray = new byte[8];
        // ByteBuffer to be backed by the buffer byte array
        ByteBuffer timestampBuf = ByteBuffer.wrap(ByteArray, 0, 4);
        ByteBuffer valueBuf = ByteBuffer.wrap(ByteArray, 4, 4);
        
        timestampBuf.putInt(timestamp);
        valueBuf.putFloat(tempVal);
        //byte array buffer content changed after put method.
        
        return ByteArray;
    }
    
    public void readFromBytes(byte [] byteArray) {
        if(byteArray.length!=TEMP_DATA_SIZE){
            System.out.println("wrong data packet size. Size need to be "+
                    TEMP_DATA_SIZE+" bytes.");
            return;
        } else{
            ByteBuffer timestampBuf = ByteBuffer.wrap(byteArray, 0, 4);
            ByteBuffer valueBuf = ByteBuffer.wrap(byteArray, 4, 4);
            timestamp = timestampBuf.getInt();
            tempVal = valueBuf.getFloat();
        }
    }

    
    public static void main(String[] args) throws Exception {
        
        TempData ld1 = new TempData();
        ld1.printTemp();
        
        TempData ld2 = new TempData(15.5f);
        ld2.printTemp();
        
        TempData ld3 = new TempData(10, 23.4f);
        ld3.printTemp();
        
        byte[] msg1 = new byte[8];
        TempData ld4 = new TempData(msg1);
        ld4.printTemp();
        
        TempData ld5 = new TempData(50, 26.5f);
        ld5.printTemp();
        ld5.writeToFile("Temp_testfile.dat");
        
        TempData ld6 = new TempData("Temp_testfile.dat");
        ld6.printTemp();
        
        TempData ld7 = new TempData(100, 8.5f);
        ld7.printTemp();
        
        byte[] msg2 = new byte[8];
        msg2 = ld7.writeToBytes();
        TempData ld8 = new TempData(msg2);
        ld8.printTemp();        
        
        System.out.println("timestamp = " + (ld8.getTime()) + "\tvalue = " + (ld8.getTemp()) + "\n");
    }
}
