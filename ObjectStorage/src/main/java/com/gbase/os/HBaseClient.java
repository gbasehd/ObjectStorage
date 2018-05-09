package com.gbase.os;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseClient {

    // 声明静态配置，配置zookeeper
    // TODO 长连接or短链接
    static Connection connection = null;
    static Logger log= Logger.getLogger("======== HBaseClient =========");
    static {
        try {
            connection = ConnectionFactory.createConnection(OSConfiguration.getHBaseConf());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     *
     * @param tableStr
     */
    public static void createTable(String tableStr, String[] familyNames) throws Exception {
        //System.out.println("start create table ......");
        try {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tableStr);
            if (admin.tableExists(tableName)) {
                throw  new Exception(String.format("%s already exists.",tableName));
                /*
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                //System.out.println(tableName + " is exist,detele....");
                */
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            // 添加表列信息
            if (familyNames != null && familyNames.length > 0) {
                for (String familyName : familyNames) {
                    tableDescriptor.addFamily(new HColumnDescriptor(familyName));
                }
            }
            admin.createTable(tableDescriptor);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //System.out.println("end create table ......");
    }

    /**
     * 添加行列数据数据
     *
     * @param tableName
     * @throws Exception
     */
    public static void insertData(String tableName, String rowId, String familyName,String qualifier, String value) throws Exception {
        //System.out.println("start insert data ......");
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowId.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        put.addColumn(familyName.getBytes(), qualifier.getBytes(), value.getBytes());// 本行数据的第一列
        try {
            table.put(put);
        } catch (IOException e) {
            throw  e;
        }
        //System.out.println("end insert data ......");
    }

    public static void insertData(String tableName, String rowId, String familyName,String qualifier, byte[] value) throws Exception {
        //System.out.println("start insert data ......");
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowId.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        put.addColumn(familyName.getBytes(), qualifier.getBytes(), value);// 本行数据的第一列
        try {
            table.put(put);
        } catch (IOException e) {
            throw  e;
        }
    }

    /**
     * 删除行
     *
     * @param tablename
     * @param rowkey
     */
    public static void deleteRow(String tablename, String rowkey) {
        try {
            Table table = connection.getTable(TableName.valueOf(tablename));
            Delete d1 = new Delete(rowkey.getBytes());
            table.delete(d1);//d1.addColumn(family, qualifier);d1.addFamily(family);
            //System.out.println("删除行成功!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询所有数据
     *
     * @param tableName
     * @throws Exception
     */
    public static void queryAll(String tableName) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                //System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (Cell keyValue : r.rawCells()) {
                    //System.out.println("列：" + new String(CellUtil.cloneFamily(keyValue))+":"+new String(CellUtil.cloneQualifier(keyValue)) + "====值:" + new String(CellUtil.cloneValue(keyValue)));
                }
            }
            rs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据rowId查询
     *
     * @param tableName
     * @throws Exception
     */
    public static Map<String,byte[]> queryByRowId(String tableName, String rowId) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Map<String,byte[]> res= new HashMap<String,byte[]>();
        try {
            Get scan = new Get(rowId.getBytes());// 根据rowkey查询
            Result r = table.get(scan);
            if(r.isEmpty()){
                throw new Exception(String.format("Object \'%s\' does not exist!",rowId));
            }else{
                res.put("rowkey", r.getRow());
                for (Cell keyValue : r.rawCells()) {
                    String family = new String(CellUtil.cloneFamily(keyValue));
                    String qualifier = new String(CellUtil.cloneQualifier(keyValue));
                    res.put(family+":"+qualifier, CellUtil.cloneValue(keyValue));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    /**
     * 根据列条件查询
     *
     * @param tableName
     */
    public static ArrayList< Map<String,String>> queryByCondition(String tableName, String familyName, String qualifier, String value) {

        ArrayList<Map<String,String>> res= new ArrayList<Map<String,String>>();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(qualifier), CompareOp.EQUAL, Bytes.toBytes(value)); // 当列familyName的值为value时进行查询
            Scan s = new Scan();
            s.setFilter(filter);
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                Map<String,String> row = new HashMap<String, String>();
                //System.out.println("获得到rowkey:" + new String(r.getRow()));
                row.put("rowkey", new String(r.getRow()));
                row.put("meta:name", new String(r.getValue(Bytes.toBytes("meta"),Bytes.toBytes("name")))) ;
                row.put("meta:path", new String(r.getValue(Bytes.toBytes("meta"),Bytes.toBytes("path")))) ;
                row.put("meta:type", new String(r.getValue(Bytes.toBytes("meta"),Bytes.toBytes("type")))) ;
                row.put("object:body", new String(r.getValue(Bytes.toBytes("object"),Bytes.toBytes("body")))) ;
                /*
                for (Cell keyValue : r.rawCells()) {
                    row.put(new String(CellUtil.cloneFamily(keyValue))+":"+new String(CellUtil.cloneQualifier(keyValue)),new String(CellUtil.cloneValue(keyValue)));
                    //System.out.println("列：" + new String(CellUtil.cloneFamily(keyValue))+":"+new String(CellUtil.cloneQualifier(keyValue)) + "====值:" + new String(CellUtil.cloneValue(keyValue)));
                }*/
                res.add(row);
            }
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    /**
     * 多条件查询
     *
     * @param tableName
     */
    public static void queryByConditions(String tableName, String[] familyNames, String[] qualifiers,String[] values) {

        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Filter> filters = new ArrayList<Filter>();
            if (familyNames != null && familyNames.length > 0) {
                int i = 0;
                for (String familyName : familyNames) {
                    Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(qualifiers[i]), CompareOp.EQUAL, Bytes.toBytes(values[i]));
                    filters.add(filter);
                    i++;
                }
            }
            FilterList filterList = new FilterList(filters);
            Scan scan = new Scan();
            scan.setFilter(filterList);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                //System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (Cell keyValue : r.rawCells()) {
                    //System.out.println("列：" + new String(CellUtil.cloneFamily(keyValue))+":"+new String(CellUtil.cloneQualifier(keyValue)) + "====值:" + new String(CellUtil.cloneValue(keyValue)));
                }
            }
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     */
    public static void dropTable(String tableStr) {
        try {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tableStr);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            admin.close();
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}