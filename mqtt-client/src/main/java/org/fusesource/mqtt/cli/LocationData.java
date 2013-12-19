/*
 * To change this locationlate, choose Tools | Locationlates
 * and open the locationlate in the editor.
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

public class LocationData {
        
    private int timestamp;
    private double latitude;
    private double longitude;
    private static final int LOC_DATA_SIZE = 20;
    
    //Constructors
    public LocationData(){
        timestamp = 0;
        latitude = 0;
        longitude = 0;
    }
    
    public LocationData(double lat, double lon){
        timestamp = (int)(new Date().getTime()/1000L);
        latitude = lat;
        longitude = lon;
    }
    
    public LocationData(int time, double lat, double lon){
        timestamp = time;
        latitude = lat;
        longitude = lon;
    }
    
    public LocationData(byte [] byteArray) {
        readFromBytes(byteArray);
    }

    public LocationData(String filename) throws IOException {
        readFromFile(filename);
    }
    
    //methods
    public double getLat (){
        return this.latitude;
    }
    
    public double getLong (){
        return this.longitude;
    }
    
    public int getTime(){
        return this.timestamp;
    }
    
    public void printLocation(){
        System.out.println("Time = "+timestamp+"; latidude = "+latitude+
                ", longitude = "+longitude);
    }
    
    public void writeToFile(String filename) throws IOException {
        byte[] byteArray = new byte[20];
        // ByteBuffer to be backed by the buffer byte array
        ByteBuffer timestampBuf = ByteBuffer.wrap(byteArray, 0, 4);
        ByteBuffer latBuf = ByteBuffer.wrap(byteArray, 4, 8);
        ByteBuffer longBuf = ByteBuffer.wrap(byteArray, 12, 8);
        
        timestampBuf.putInt(timestamp);
        latBuf.putDouble(latitude);
        longBuf.putDouble(longitude);
        //byte array buffer content changed after put method.
        
        FileOutputStream file = new FileOutputStream(filename);
        file.write(byteArray); //input must be byte[]. Not byteBuffer
        file.close();
    }
    
    public void readFromFile(String filename) throws IOException {
        byte[] byteArray = new byte[20];
        ByteBuffer timestampBuf = ByteBuffer.wrap(byteArray, 0, 4);
        ByteBuffer latBuf = ByteBuffer.wrap(byteArray, 4, 8);
        ByteBuffer longBuf = ByteBuffer.wrap(byteArray, 12, 8);
        
        FileInputStream file = new FileInputStream(filename);
        file.read(byteArray);
        file.close();
        
        timestamp = timestampBuf.getInt();
        latitude = latBuf.getDouble();
        longitude = longBuf.getDouble();
    }
    
    public byte[] writeToBytes() {
        byte[] byteArray = new byte[20];
        // ByteBuffer to be backed by the buffer byte array
        ByteBuffer timestampBuf = ByteBuffer.wrap(byteArray, 0, 4);
        ByteBuffer latBuf = ByteBuffer.wrap(byteArray, 4, 8);
        ByteBuffer longBuf = ByteBuffer.wrap(byteArray, 12, 8);
        
        timestampBuf.putInt(timestamp);
        latBuf.putDouble(latitude);
        longBuf.putDouble(longitude);
        //byte array buffer content changed after put method.
        
        return byteArray;
    }
    
    public void readFromBytes(byte [] byteArray) {
        if(byteArray.length!=LOC_DATA_SIZE){
            System.out.println("wrong data packet size. Size need to be "+
                    LOC_DATA_SIZE+" bytes.");
            return;
        } else{
            ByteBuffer timestampBuf = ByteBuffer.wrap(byteArray, 0, 4);
            ByteBuffer latBuf = ByteBuffer.wrap(byteArray, 4, 8);
            ByteBuffer longBuf = ByteBuffer.wrap(byteArray, 12, 8);

            timestamp = timestampBuf.getInt();
            latitude = latBuf.getDouble();
            longitude = longBuf.getDouble();
        }
    }

    
    public static void main(String[] args) throws Exception {
        
        LocationData ld1 = new LocationData();
        System.out.print("ld1: "); ld1.printLocation();
        
        LocationData ld2 = new LocationData(15.5d, 189.89d);
        System.out.print("ld2: "); ld2.printLocation();
        
        LocationData ld3 = new LocationData(10, 189.89d, 189.89d);
        System.out.print("ld3: "); ld3.printLocation();
        
        byte[] msg1 = new byte[LocationData.LOC_DATA_SIZE];
        LocationData ld4 = new LocationData(msg1);
        System.out.print("ld4: "); ld4.printLocation();
        
        LocationData ld5 = new LocationData(50, 123.9d, 7634.8d);
        System.out.print("ld5: "); ld5.printLocation();
        ld5.writeToFile("Loc_testfile.dat");
        
        LocationData ld6 = new LocationData("Loc_testfile.dat");
        System.out.print("ld6: "); ld6.printLocation();
        
        LocationData ld7 = new LocationData(100, 8.455d, 4655.4d);
        System.out.print("ld7: "); ld7.printLocation();
        
        byte[] msg2 = new byte[8]; //test with wrong size, no use, it will be re-assigned next line
        msg2 = ld7.writeToBytes();
        LocationData ld8 = new LocationData(msg2);
        System.out.print("ld8: "); ld8.printLocation();
        
        msg2[1] = (byte)90;
        ld7.readFromBytes(msg2);
        ld8.readFromBytes(msg2);
        System.out.print("ld7: "); ld7.printLocation();  
        System.out.print("ld8: "); ld8.printLocation();  
        
        System.out.println("timestamp = " + (ld8.getTime()) + "\tvalue = " +
                (ld8.getLat()) +", "+ld8.getLong()+"\n");
    }
}

//ByteBuffer bb;
//...
//byte[] contentsOnly = Arrays.copyOf( bb.array(), bb.position() );