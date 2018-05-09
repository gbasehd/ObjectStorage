package com.gbase.os;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GetObject extends Object {

    private Logger log= Logger.getLogger("GetObject");
    private boolean readHBase = false;
    private byte[] bodyBuffer = null;
    private int next = 0;
    private long pos = 0;

    public GetObject(String bucket,String name){
        super(bucket,name);
    }

    @Override
    public String getPath() {
        if(!readHBase){
            try{
                getFromHBase();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return super.getPath();
    }

    @Override
    public long getSize() {
        if(!readHBase){
            try{
                getFromHBase();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return super.getSize();
    }

    @Override
    public String getType() {
        if(!readHBase){
            try{
                getFromHBase();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return super.getType();
    }

    @Override
    public int get(byte[] buffer) throws Exception {
        int size = -1;
        log.info(String.format("THRESHOLD : %d", THRESHOLD));
        if(!readHBase){ getFromHBase(); }
        if(this.type.equals("L")){
            size = get(buffer,0,buffer.length);
            log.info(String.format("read from hbase length=%d read=%d size=%d",buffer.length,size,bodyBuffer.length));
        }else{
            size = hdfsCli.readFile(getPath(),pos,buffer,0,buffer.length);
            log.info(String.format("read form hdfs pos=%d length=%d read=%d",pos,buffer.length,size));
            if(size != -1){ pos += size; }
        }
        return size;
    }

    private int get(byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        byte c = get();
        if( c==0 && next >= bodyBuffer.length){
            return -1;
        }

        b[off] = c;

        int i = 1;
        for (; i < len ; i++) {
            c = get();
            if (c == 0 && next >= bodyBuffer.length) {
                break;
            }
            b[off + i] = c;
        }
        return i;
    }

    private byte get(){
        if(next >= bodyBuffer.length){
            return 0;
        }else{
            return bodyBuffer[next++];
        }
    }

    private void getFromHBase() throws Exception{
        Map<String,byte[]> res= new HashMap<String,byte[]>();
        res = hbaseCli.queryByRowId(this.bucket, this.name);
        this.type = new String(res.get(META_FAMILY+":"+TYPE_QUALIFIE));
        this.path = new String(res.get(META_FAMILY+":"+PATH_QUALIFIE));
        this.bodyBuffer = res.get(OBJECT_FAMILY+":"+BODY_QUALIFIE);
        log.info(String.format("bodyBuffer size=%d",bodyBuffer.length));
        this.totalSize= Long.parseLong(new String(res.get(META_FAMILY+":"+Size_QUALIFIE)));
        this.readHBase = true;
    }
}
