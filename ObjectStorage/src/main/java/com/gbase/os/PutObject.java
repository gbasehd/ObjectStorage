package com.gbase.os;

import org.apache.log4j.Logger;
import java.util.Arrays;

public class PutObject extends Object {

    private  static String HDFS_ROOT = "/os/big/";
    private  Logger log= Logger.getLogger("PutObject");
    private  boolean hasPutMeta = false;
    private  boolean firstPut = true;

    static{
        try{
            HDFS_ROOT = OSConfiguration.getOSConf("os.hdfs.root.dir");
        }catch(Exception e){ }
    }

    public PutObject(String bucket,String name,long size){
        super(bucket,name,size);
    }

    @Override
    public void put(byte[] buffer, long size) throws Exception {
        if(size > buffer.length){
            throw new Exception(String.format("Actual size(%d) is more than buffer size(%d)",size,buffer.length));
        }
        log.info(String.format("THRESHOLD : %d", THRESHOLD));
        if(this.totalSize < THRESHOLD) {
            if(this.totalSize == size ){
                byte[] tmpBuf = Arrays.copyOf(buffer, (int) size);
                if(!hasPutMeta){putMetaSmall();}
                putBodySmall(tmpBuf);
            }else{
                throw new Exception(String.format("The size of this write(%d) does not match the total size(%d) !",size,this.totalSize));
            }
        }else{
            if(size <= totalSize){
                byte[] tmpBuf = Arrays.copyOf(buffer, (int) size);
                if(!hasPutMeta){ putMetaBig(); }
                putBodyBig(tmpBuf);
            }
        }
    }

    @Override
    public String getPath() {
        if(getSize() > THRESHOLD){
            return new String(HDFS_ROOT+"/"+name);
        }else{
            return "NULL";
        }
    }

    @Override
    public String getType() {
        if(getSize() > THRESHOLD){
            return "B";
        }else{
            return "L";
        }
    }

    private void putMetaBig() throws Exception{
        log.info(String.format("Put meta of big object \'%s\' into hbase(name:%s,path:%s,type:B,size:%d,body:NULL)",
                this.name,this.name,getPath(),this.totalSize));
        String rowkey = this.name;
        String tableName = this.bucket;
        hbaseCli.insertData(tableName, rowkey, META_FAMILY, NAME_QUALIFIE, this.name);
        hbaseCli.insertData(tableName, rowkey, META_FAMILY, PATH_QUALIFIE, getPath());
        hbaseCli.insertData(tableName, rowkey, META_FAMILY, TYPE_QUALIFIE, "B");
        hbaseCli.insertData(tableName, rowkey, META_FAMILY, Size_QUALIFIE, Long.toString(this.totalSize));
        hbaseCli.insertData(tableName, rowkey, OBJECT_FAMILY, BODY_QUALIFIE, "NULL");
        hasPutMeta = true;
    }

    private void putMetaSmall() throws Exception{
        log.info(String.format("Put meta of small object \'%s\' into hbase(name:%s,path:NULL,type:L,size:%d)",
                this.name,this.name,this.totalSize));
        String rowkey = this.name;
        String tableName = this.bucket;
        hbaseCli.insertData(tableName, rowkey, META_FAMILY, NAME_QUALIFIE, this.name);
        hbaseCli.insertData(tableName, rowkey, META_FAMILY, PATH_QUALIFIE, "NULL");
        hbaseCli.insertData(tableName, rowkey, META_FAMILY, TYPE_QUALIFIE, "L");
        hbaseCli.insertData(tableName, rowkey, META_FAMILY, Size_QUALIFIE, Long.toString(this.totalSize));
        hasPutMeta = true;
    }

    private void putBodySmall(byte[] buffer) throws Exception{
        log.info(String.format("Put body of small object \'%s\' into hbase(size:%d)",this.name,buffer.length));
        log.info(String.format("buffer size=%d ",buffer.length));
        String rowkey = this.name;
        String tableName = this.bucket;
        hbaseCli.insertData(tableName, rowkey, OBJECT_FAMILY, BODY_QUALIFIE,buffer);
    }

    private void putBodyBig(byte[] buffer) throws Exception{
        log.info(String.format("Put body of big object \'%s\' into hdfs(size:%d)",this.name,buffer.length));
        hdfsCli.appendFile(getPath(),buffer,firstPut);
        firstPut = false;
    }
}
