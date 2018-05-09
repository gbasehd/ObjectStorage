package com.gbase.os;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

public class HDFSClient {
    //initialization
    private  static Logger log= Logger.getLogger("HDFSClient");
    static FileSystem hdfs;
    static {
        try {
            hdfs = FileSystem.get(URI.create(OSConfiguration.getHDFSConf("fs.defaultFS")),
                    OSConfiguration.getHDFSConf());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //create a direction
    public void createDir(String dir) throws IOException {
        Path path = new Path(dir);
        hdfs.mkdirs(path);
        log.info("new dir \t" + OSConfiguration.getHDFSConf("fs.defaultFS") + dir);
    }

    //copy from local file to HDFS file
    public void copyFile(String localSrc, String hdfsDst) throws IOException{
        Path src = new Path(localSrc);
        Path dst = new Path(hdfsDst);
        log.info("begin to copy...");
        hdfs.copyFromLocalFile(src, dst);

        //list all the files in the current direction
        FileStatus files[] = hdfs.listStatus(dst);
        log.info("Upload to \t" + OSConfiguration.getHDFSConf("fs.defaultFS") + hdfsDst);
        for (FileStatus file : files) {
            log.info(file.getPath());
        }
    }

    //create a new file
    //public void createFile(String fileName, String fileContent) throws IOException {
    public void createFile(String fileName, byte[] bytes) throws IOException {
        Path dst = new Path(fileName);
        //byte[] bytes = fileContent.getBytes();
        FSDataOutputStream output = hdfs.create(dst);
        output.write(bytes);
        output.close();
        log.info("new file \t" + OSConfiguration.getHDFSConf("fs.defaultFS") + fileName);
    }

    public void appendFile(String fileName, byte[] bytes, boolean first) throws IOException {
        FSDataOutputStream output = null;
        try{
            Path dst = new Path(fileName);
            boolean isExists = hdfs.exists(dst);
            if(first){
                if(isExists){
                    hdfs.delete(dst,true);
                    log.info("delete file \t" + OSConfiguration.getHDFSConf("fs.defaultFS") + fileName);
                }
                output = hdfs.create(dst);
                output.write(bytes);
                output.flush();
                log.info("new file \t" + OSConfiguration.getHDFSConf("fs.defaultFS") + fileName);
            }else{
                output = hdfs.append(dst);
                output.write(bytes);
                output.flush();
                log.info("append file \t" + OSConfiguration.getHDFSConf("fs.defaultFS") + fileName);
            }
        }finally {
            if(output != null){
                output.close();
            }
        }
    }

    public int readFile(String filePath, long pos, byte[] buffer, int offset, int length ) throws IOException {
        FSDataInputStream in = null;
        int read = -1;
        try {
            Path srcPath = new Path(filePath);
            in = hdfs.open(srcPath);
            read = in.read(pos,buffer,offset,length);
        } finally {
            if(null != in){
                in.close();
            }
        }
        return read;
    }

    public void readFile(String filePath) throws IOException {
        InputStream in = null;
        try {
            Path srcPath = new Path(filePath);
            in = hdfs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 100, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    //list all files
    public void listFiles(String dirName) throws IOException {
        Path f = new Path(dirName);
        FileStatus[] status = hdfs.listStatus(f);
        log.info(dirName + " has all files:");
        for (int i = 0; i< status.length; i++) {
            log.info(status[i].getPath().toString());
        }
    }

    //judge a file existed? and delete it!
    public void deleteFile(String fileName) throws IOException {
        Path f = new Path(fileName);
        boolean isExists = hdfs.exists(f);
        if (isExists) {	//if exists, delete
            boolean isDel = hdfs.delete(f,true);
            log.info(fileName + "  delete? \t" + isDel);
        } else {
            log.info(fileName + "  exist? \t" + isExists);
        }
    }
}
