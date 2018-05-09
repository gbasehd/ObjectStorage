package com.gbase.os;

import org.apache.log4j.Logger;
import java.io.*;

public class ObjectStorage {
    public static void main(String[] args) throws Exception {
        Logger log= Logger.getLogger("======== os =========");
        int bufferSize = 10485760; //10MB = 10*1024*1024

        try{
            String bucketName = args[1];
            Bucket bucket = new Bucket(bucketName);
            if(args[0].equals("create")){
                log.info(String.format("begin to create bucket \'%s\'...",bucketName));
                bucket.create();
                log.info("done");
            }
            if(args[0].equals("drop")){
                log.info(String.format("begin to drop bucket \'%s\'...",bucketName));
                bucket.drop();
                log.info("done");
            }
            if(args[0].equals("put")){
                String objectName = args[2];
                log.info(String.format("begin to put object \'%s\' into bucket \'%s\'...",objectName,bucketName));
                File file = new File(objectName);
                InputStream in = null;
                long fileLen = file.length();
                System.out.println(String.format("文件大小：%d)",fileLen));
                byte[] tempbytes = new byte[bufferSize];
                try {
                    int byteread = 0;
                    in = new FileInputStream(objectName);
                    Object o = new PutObject(bucketName,objectName,fileLen);
                    log.info(String.format("Threshold is \'%d\' ",o.getThreshold()));
                    while ((byteread = in.read(tempbytes)) != -1) {
                       o.put(tempbytes,byteread) ;
                    }
                    log.info("done");
                } catch (Exception e1) {
                    throw e1;
                } finally {
                    if (in != null) {
                        try {
                            in.close();
                        } catch (IOException e1) {
                        }
                    }
                }
            }
            if(args[0].equals("get")){
                String objectName = args[2];
                log.info(String.format("begin to get object \'%s\' from bucket \'%s\'...",objectName,bucketName));
                Object o = new GetObject(bucketName,objectName);
                log.info(String.format("Threshold is \'%d\' ",o.getThreshold()));
                int byteread = 0;
                byte[] buffer = new byte[bufferSize];
                OutputStream out = null;
                try{
                    File file = new File("test/" + objectName);
                    out = new FileOutputStream(file,true);
                    while((byteread = o.get(buffer)) != -1){
                        out.write(buffer,0,byteread);
                    }
                    log.info(String.format("Get meta of object \'%s\'(name:%s,type:%s,size:%d,path:%s)",
                            o.getName(), o.getName(),o.getType(),o.getSize(),o.getPath()));
                    log.info("Done");
                }catch (Exception e){
                    throw e;
                }finally {
                    if (out != null) {
                        try { out.close(); }
                        catch (IOException e1) { }
                    }
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /*
    public static void testHBaseClient() throws Exception{
        HBaseClient client = new HBaseClient();
        //创建表
        client.createTable("t_table", new String[]{"f1","f2","f3"});
        //添加数据
        client.insertData("t_table", "row-0001", "f1","a", "fffaaa");
        client.insertData("t_table", "row-0001", "f2", "b","fffbbb");
        client.insertData("t_table", "row-0001", "f3", "c","fffccc");
        client.insertData("t_table", "row-0002", "f1", "a","eeeeee");
        //查询全部数据
        client.queryAll("t_table");
        //根据rowid查询数据
        client.queryByRowId("t_table", "row-0001");
        //列条件查询
        client.queryByCondition("t_table", "f1","a", "eeeeee");
        //多条件查询
        client.queryByConditions("t_table", new String[]{"f1","f3"},new String[]{"a","c"}, new String[]{"fffaaa","fffccc"});
        //删除记录
        client.deleteRow("t_table", "row-0001");
        //删除表
        client.dropTable("t_table");
    }*/
}
