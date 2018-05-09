package com.gbase.os;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class OSConfiguration {
    private static Configuration hdfsConf = new Configuration();
    private static Configuration osConf = new Configuration();
    private static Configuration hbaseConf =  HBaseConfiguration.create();
    static {
        PropertyConfigurator.configure("../conf/log4j.properties");
        String hadoopPath =  null;
        String hbaseMaster =  null ;
        String hbaseZooKeeper = null;
        String hbaseZookeeperPort =   null;
        String hbaseZNodeParent =  null;
        Logger log= Logger.getLogger("OSConfiguration");
        try{
            osConf.addResource(new Path("../conf" , "os-core.xml"));

            hadoopPath = osConf.get("os.hdfs.conf.dir");
            hbaseMaster =osConf.get("os.hbase.master");
            hbaseZooKeeper = osConf.get("os.hbase.zookeeper.quorum");
            hbaseZookeeperPort =  osConf.get("os.hbase.zookeeper.property.clientPort");
            hbaseZNodeParent = osConf.get("os.zookeeper.znode.parent");

            log.info("Read configuration from os-core.xml.");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(hadoopPath == null){ hadoopPath =  "/usr/ghd/current/hadoop-client/conf"; }
            hdfsConf.addResource(new Path(hadoopPath, "core-site.xml"));
            hdfsConf.addResource(new Path(hadoopPath, "hdfs-site.xml"));
            hdfsConf.addResource(new Path(hadoopPath, "mapred-site.xml"));

            if(hbaseMaster == null){ hbaseMaster =  "c1.gbase.cn" ; }
            hbaseConf.set("hbase.master",hbaseMaster);

            if(hbaseZooKeeper==null){ hbaseZooKeeper = "c1.gbase.cn"; }
            hbaseConf.set("hbase.zookeeper.quorum", hbaseZooKeeper);

            if(hbaseZookeeperPort==null){ hbaseZookeeperPort =   "2181"; }
            hbaseConf.set("hbase.zookeeper.property.clientPort",hbaseZookeeperPort);

            if(hbaseZNodeParent==null){ hbaseZNodeParent =  "/hbase-unsecure"; }
            hbaseConf.set("zookeeper.znode.parent",hbaseZNodeParent);

            log.info(String.format("\nhadoopPath=%s \nhbaseMaster=%s \nhbaseZookeeper=%s \nhbaseZookeeperPort=%s " +
                            "\nhbaseZNodeParent=%s \nhdfsRootDir=%s \nobjectSizeThreshold=%s",
                   hadoopPath,hbaseMaster,hbaseZooKeeper,hbaseZookeeperPort,hbaseZNodeParent,
                    osConf.get("os.hdfs.root.dir"), osConf.get("os.object.size.threshold")));
        }
    }

   public static String getHDFSConf(String key){
        return hdfsConf.get(key);
   }

   public static Configuration getHDFSConf(){
       return hdfsConf;
   }

   public static Configuration getHBaseConf(){
       return hbaseConf;
   }

   public static String getHBaseConf(String key){
       return hbaseConf.get(key);
   }

    public static String getOSConf (String key){
        return osConf.get(key);
    }
}
