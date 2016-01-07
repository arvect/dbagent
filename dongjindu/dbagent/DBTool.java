/*
 * Copyright (c) 2014, Dong Jin Du
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package dongjindu.dbagent;

import com.google.common.collect.TreeTraverser;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Yetaai
 */
public class DBTool {
    private final static Logger logger = LoggerFactory.getLogger(DBTool.class);
    private static final HashMap<String, DataSource> hmDataSource = new HashMap();

    private static int batchSize = 30000;

    public final static String MAINDBCONN = "main";
    public final static String TESTDBCONN = "hp110";
    public final static String LOCALTESTDBCONN = "localtest";
    
    private static String mainEntry;
    public static void setMainEntry(String pMainEntry) {
        mainEntry = pMainEntry;
    } 
    static {
//        BasicConfigurator.configure();
//        ResourceBundle config;
//        FileInputStream fis;
        try {
//            fis = new FileInputStream(DBTool.class.getClassLoader().getResource("DBProperties.properties").getPath());
//            System.out.println("Config file directory is: " + DBTool.class.getClassLoader().getResource("DBProperties.properties").getPath());
//            config = new PropertyResourceBundle(fis); //ResourceBundle.getBundle("DBProperties.properties");
//            fis.close();
            String DRIVER_NAME = "com.mysql.jdbc.Driver"; //config.getString("com.mysql.jdbc.Driver");
            String URL = "jdbc:mysql://localhost:3306/findOpp"; //config.getString("jdbc:mysql:/localhost:3306/invest001");
            String UNAME = "***"; //config.getString("root");
            String PWD = "****"; //config.getString("elttil");
            hmDataSource.put(DBTool.MAINDBCONN, setupDataSource(URL, DRIVER_NAME, UNAME, PWD));

            String DRIVER_NAME2 = "com.mysql.jdbc.Driver"; //config.getString("com.mysql.jdbc.Driver");
            String URL2 = "jdbc:mysql://localhost:3306/findOppTest"; //config.getString("jdbc:mysql:/localhost:3306/invest001");
            String UNAME2 = "***"; //config.getString("root");
            String PWD2 = "****"; //config.getString("elttil");
            hmDataSource.put(DBTool.LOCALTESTDBCONN, setupDataSource(URL2, DRIVER_NAME2, UNAME2, PWD2));

            String URL1 = "jdbc:mysql://192.168.0.110:3306/findOppTest";//hp notebook c2630
            String DRIVER_NAME1 = "com.mysql.jdbc.Driver";
            String UNAME1 = "***";
            String PWD1 = "*****";
            hmDataSource.put(DBTool.TESTDBCONN, setupDataSource(URL1, DRIVER_NAME1, UNAME1, PWD1));
//            System.out.println("*******************************DB URL: " + URL + " *****************************");
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        } finally {
        }
    }

    public static void setBatchSize(int i) {
        batchSize = i;
    }

    public static int getBatchSize() {
        return batchSize;
    }

    //Mysql Only
    public static Connection getMainConn(boolean autocommit) {
        try {
            String connString = null;
            if (mainEntry != null && mainEntry.toLowerCase().contains("nctest")) {
                connString = DBTool.TESTDBCONN;
            } else if (mainEntry != null && mainEntry.toLowerCase().contains("localtest")) {
                connString = DBTool.LOCALTESTDBCONN;
            } else if (mainEntry != null && mainEntry.toLowerCase().contains("test")) {
                connString = DBTool.TESTDBCONN;
            } else {
                connString = DBTool.MAINDBCONN;
            }
            Connection conn = hmDataSource.get(connString).getConnection();//dataSource.getConnection();
            conn.setAutoCommit(autocommit);
//            System.out.println("Connection get. Client info: " + hmDataSource.get(connString).toString());
            return conn;
        } catch (SQLException ex) {
            logger.error("Cannot get main connection!");
            ex.printStackTrace();
        }
        return null;
    }

    public static Connection getConn(String pDataSourceName, boolean autocommit) {
        try {
            Connection conn = hmDataSource.get(pDataSourceName).getConnection();
            conn.setAutoCommit(autocommit);
            return conn;
        } catch (SQLException ex) {
            logger.error("Cannot get connection! name: " + pDataSourceName);
            ex.printStackTrace();
        }
        return null;
    }

    private static DataSource setupDataSource(String URL, String DRIVER_NAME,
            String UNAME, String PWD) {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass(DRIVER_NAME);
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }
        cpds.setJdbcUrl(URL);
        cpds.setUser(UNAME);
        cpds.setPassword(PWD);
        cpds.setMinPoolSize(5);
        cpds.setAcquireIncrement(5);
        cpds.setMaxPoolSize(10);
        return cpds;
    }

    private static void putDataSource(String pName, DataSource pDataSource) {
        hmDataSource.put(pName, pDataSource);
    }

    public static void copyTable(String pTableName, String pFromDataSource, String pToDataSource, boolean pDropDestTable) {
        //    Connection fromConn = DBTool.getConn(pFromDataSource, true);
        //    Connection toConn = DBTool.getConn(pToDataSource, true);
        try {
            copyTable(pTableName, DBTool.getConn(pFromDataSource, true),
                    DBTool.getConn(pToDataSource, true),
                    batchSize,
                    pDropDestTable);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void copyTable(String pTableName, Connection pFromConn, Connection pToConn, int pBatchSize, boolean pDropDestTable) {
        try {
            DatabaseMetaData meta = pToConn.getMetaData();
            String sver = meta.getDatabaseProductVersion();
//            System.out.println("Server version string: " + sver);
            if (sver.indexOf("-") > 0) {
                sver = sver.substring(0, sver.indexOf("-")).trim();
            } 
            String[] saver = sver.split("\\.");
            int[] iaver = new int[saver.length];
            for (int i = 0; i < saver.length; i++) {
                try {
                iaver[i] = Integer.valueOf(saver[i]);
                } catch (Exception e) {
                    System.out.println("Not a number analyzing version string of database. try discard last character.");
                    iaver[i] = Integer.valueOf(saver[i].substring(0, saver[i].length() - 1));
                }
            }
            boolean supportBtree = true;
            if ((iaver.length > 0 && iaver[0] < 5) || (iaver.length > 1 && iaver[0] == 5 && iaver[1] < 1)) {
                supportBtree = false;
            }
            if (pBatchSize == 0) {
                pBatchSize = Integer.MAX_VALUE;
            } else if (pBatchSize < 0) {
                throw new Exception("Use a positive value for copy batch size");
            }
            if (pDropDestTable) {
                PreparedStatement psDrop = pToConn.prepareStatement("drop table if exists " + pTableName);
                psDrop.execute();
            }

            PreparedStatement ps = pFromConn.prepareStatement("show create table " + pTableName);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                if (rs.getString(2).contains("USING BTREE") && !supportBtree) {
                    System.out.println("Not creatting this table because of BTree version compatibility: " + pTableName);
                    rs.close();
                    return;
                }
                PreparedStatement psCreate = pToConn.prepareStatement(rs.getString(2));
                psCreate.execute();
            }
            rs.close();

            PreparedStatement psCount = pFromConn.prepareStatement("select count(*) from " + pTableName);
            ResultSet rsCnt = psCount.executeQuery();
            int rowCnt = 0;
            while (rsCnt.next()) {
                rowCnt = rsCnt.getInt(1);
            }
            rsCnt.close();
            int currentRow = 0;
            ResultSetMetaData rsMeta = null;
            StringBuilder copyInsert = new StringBuilder("insert into " + pTableName + " (");
            PreparedStatement psSource = pFromConn.prepareStatement("select * from " + pTableName + " limit ?, ?");
            PreparedStatement psCopyInsert = null;
            int l = 0;
            while (currentRow < rowCnt) {
                l++;
                psSource.setInt(1, currentRow);
                psSource.setInt(2, pBatchSize);
                currentRow = currentRow + pBatchSize;
                ResultSet rsSource = psSource.executeQuery();
                if (l == 1) {
                    rsMeta = rsSource.getMetaData();

                    for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                        copyInsert.append(rsMeta.getColumnName(i) + ", ");
                    }
                    copyInsert = new StringBuilder(copyInsert.substring(0, copyInsert.length() - 2) + ") values(");
                    for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                        copyInsert.append("?, ");
                    }
                    copyInsert = new StringBuilder(copyInsert.substring(0, copyInsert.length() - 2) + ")");

                    psCopyInsert = pToConn.prepareStatement(copyInsert.toString());
                }

                int fc = 0;
                while (rsSource.next()) {
                    for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                        psCopyInsert.setObject(i, rsSource.getObject(i));
                        fc++;
                    }
                    psCopyInsert.addBatch();
                    if (fc > batchSize / rsMeta.getColumnCount()) {
                        psCopyInsert.executeBatch();
                    }
                }
                psCopyInsert.executeBatch();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void copyTables(String pFromDataSource, String pToDataSource, String pTablePattern, boolean pDropDest) {
        final HashMap<String, Integer> hmCopied = new HashMap();
        try (Connection fromConn = DBTool.getConn(pFromDataSource, true);
                Connection toConn = DBTool.getConn(pToDataSource, true)) {
            DatabaseMetaData fromMeta = fromConn.getMetaData();
            String[] tableTypes = {"Table"};
            ResultSet rsMeta = fromMeta.getTables(null, null, "%", tableTypes);
            ArrayList<TableNode> alTNode = new ArrayList();
            while (rsMeta.next()) {
                alTNode.add(new TableNode(rsMeta.getString("TABLE_NAME")));
            }
            HashMap<String, TableNode> hmAl = new HashMap();
            for (TableNode tn : alTNode) {
                hmAl.put(tn.tableName, tn);
            }

            TableNode root = new TableNode(null);
            rsMeta.beforeFirst();
            int k = 0;
            while (rsMeta.next()) {
                ResultSet rsImpKMeta = fromMeta.getImportedKeys(null, null, rsMeta.getString("TABLE_NAME"));
                int i = 0;
                while (rsImpKMeta.next()) {
                    i++;
                }
                if (i == 0) {
                    k++;
                    ((ArrayList) root.subTNodes).add(hmAl.get(rsMeta.getString("TABLE_NAME")));
                }
            }
            for (int i = 0; i < alTNode.size(); i++) {
                ResultSet rsExpKMeta = fromMeta.getExportedKeys(null, null, alTNode.get(i).tableName);
                HashMap<String, String> hm = new HashMap();
                while (rsExpKMeta.next()) {
                    hm.put(rsExpKMeta.getString("FKTABLE_NAME"), "abc");
                }
                ArrayList al = alTNode.get(i).subTNodes;
                for (String s : hm.keySet()) {
                    if (s != null) {
                        al.add(hmAl.get(s));
                    }
                }
            }
            TreeTraverser<TableNode> tra = new TreeTraverser<TableNode>() {
                @Override
                public Iterable<TableNode> children(TableNode pRoot) {
                    return pRoot.subTNodes;
                }
            };

            for (TableNode node : tra.breadthFirstTraversal(root)) {
                if (node.tableName == null) {
                    continue;
                }
                if (node.tableName.matches(pTablePattern) && hmCopied.get(node.tableName) == null) {
                    System.out.println("Copying " + node.tableName);
                    DBTool.copyTable(node.tableName, fromConn, toConn, batchSize, pDropDest);
                    hmCopied.put(node.tableName, 1);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param pDataSource
     * @return is the number of tables that cannot be dropped.
     */
    public static int dropAll(String pDataSource) {
        int lastCnt = 0;
        int thisCnt = 1;
        while (true) {
            thisCnt = dropAllOneRound(pDataSource);
            if (thisCnt == lastCnt) {
                break;
            }
            lastCnt = thisCnt;
        }
        return thisCnt;
    }

    private static int dropAllOneRound(String pDataSource) {
        try (Connection conn = DBTool.getConn(pDataSource, true)) {
            DatabaseMetaData meta = conn.getMetaData();
            String[] types = {"Table"};
            ResultSet rs = meta.getTables(null, null, "%", types);
            int i = 0;
            while (rs.next()) {
                i++;
                try {
                    String sdrop = "drop table if exists " + rs.getString("TABLE_NAME");
                    PreparedStatement ps = conn.prepareStatement(sdrop);
                    ps.executeUpdate();
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
            }
            return i;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static void main(String args[]) {
//        DBTool.setBatchSize(1000);
//        DBTool.copyTable("properties", "main", "hp107", true);
//        DBTool.dropAll("hp110");
        copyTables(DBTool.MAINDBCONN, DBTool.LOCALTESTDBCONN, ".*", true);
    }
}
