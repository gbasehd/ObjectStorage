package com.gbase.os;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.log4j.Logger;

public class Object {
    protected static int THRESHOLD = 10485760; // 10MB
    static {
        try {
            THRESHOLD = Integer.parseInt(OSConfiguration.getOSConf("os.object.size.threshold"));
        } catch (NumberFormatException e) {
            //e.printStackTrace();
        }
    }
    protected HDFSClient hdfsCli = new HDFSClient();
    protected HBaseClient hbaseCli = new HBaseClient();
    protected String name=null;
    protected String type = null;
    protected String path = null;
    protected String bucket = null;
    protected long totalSize = 0;
    protected static String META_FAMILY = "meta";
    protected static String OBJECT_FAMILY = "object";
    protected static String NAME_QUALIFIE = "name";
    protected static String PATH_QUALIFIE = "path";
    protected static String TYPE_QUALIFIE = "type";
    protected static String Size_QUALIFIE = "size";
    protected static String BODY_QUALIFIE = "body";

    public Object(String bucket, String name, long size){
        this.bucket = bucket;
        this.name = name;
        this.totalSize = size;
    }

    public int getThreshold(){
        return THRESHOLD;
    }

    public Object(String bucket, String name){
        this.bucket = bucket;
        this.name = name;
    }

    public void put(byte[] buffer,long size) throws Exception{ }
    public int get(byte[] buffer) throws Exception{ return 0; }
    public String getName(){
        return this.name;
    }
    public long getSize(){ return totalSize; }
    public String getPath(){
        return path;
    }
    public String getType(){
        return type;
    }

}

