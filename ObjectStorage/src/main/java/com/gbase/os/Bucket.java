
package com.gbase.os;

public class Bucket {
    public static int GID = 100;
    private String tableName;
    private HBaseClient client = new HBaseClient();

    public Bucket(String name){
        this.tableName = name;
    }

    public void create() throws Exception{
        client.createTable(tableName, new String[]{"object","meta"});
    }

    public void drop() throws Exception{
        client.dropTable(tableName);
    }

    /*
    //TODO 1)多线程安全 2)rowkey性能优化设计
    private String getUid(){
        GID++;
        long now = System.currentTimeMillis();
        //获取4位年份数字
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy");
        //获取时间戳
        String time=dateFormat.format(now);
        String info=now+"";
        int ran=0;
        if(GID>999){ GID=100; }
        ran=GID;
        return time+info.substring(2, info.length())+ran;
    }*/
}
