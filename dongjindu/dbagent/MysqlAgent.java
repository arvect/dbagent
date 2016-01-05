/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dongjindu.dbagent;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author oefish
 */
public class MysqlAgent<T> implements DBAgent<T> {

    static final private Logger logger = LoggerFactory.getLogger(MysqlAgent.class);

    private Class<T> clazz;
    public String TABLENAME;
    public String[] TABLEKEY;

    public String createSQL;
    public String replaceSQL;
    public String delPkSQL;
    public Field[] pojoFields;
    public int[] pojoFieldsPos;
    public int[] pojoFieldsLength;
    public HashMap<String, Integer> hmLength;

    private Field[] genPojoFields(Class<T> pClazz) {
        Field[] f = clazz.getDeclaredFields();
        ArrayList<Field> af = new ArrayList();
        for (int i = 0; i < f.length; i++) {
            if (!Modifier.isStatic(f[i].getModifiers())
                    && (f[i].getType().equals(String.class) || f[i].getType().equals(Double.class)
                    || f[i].getType().equals(Long.class) || f[i].getType().equals(Double.TYPE)
                    || f[i].getType().equals(Long.TYPE) || f[i].getType().equals(Boolean.class)
                    || f[i].getType().equals(Boolean.TYPE))) {
                if (f[i].getName().charAt(0) == '_') {
                    continue;
                }
                af.add(f[i]);
            }
        }
        Field[] lvPojoFields = new Field[af.size()];
        lvPojoFields = af.toArray(lvPojoFields);
        return lvPojoFields;
    }

    public MysqlAgent(Class<T> pClazz, String pTableName, String[] pTableKey, HashMap<String, Integer> pHmLength) {
        clazz = pClazz;
        TABLENAME = pTableName;
        TABLEKEY = pTableKey;
        hmLength = pHmLength;
        pojoFields = this.genPojoFields(pClazz);

        pojoFieldsPos = new int[pojoFields.length];
        for (int i = 0; i < pojoFields.length; i++) {
            pojoFieldsPos[i] = i;
        }
        if (hmLength != null && !hmLength.isEmpty()) {
            pojoFieldsLength = new int[pojoFields.length];
            for (int i = 0; i < pojoFields.length; i++) {
                if (hmLength.get(pojoFields[i].getName()) != null) {
                    pojoFieldsLength[i] = hmLength.get(pojoFields[i].getName());
                } else {
                    hmLength.put(pojoFields[i].getName(), -1);
                    pojoFieldsLength[i] = -1;
                }
            }
        }
        this.createSQL = this.genCreateSQL();
        this.replaceSQL = this.genReplaceSQL();
        this.delPkSQL = this.genDelPkSQL();
    }

    public MysqlAgent(Class<T> pClazz, String pTableName, String[] pTableKey, HashMap<String, Integer> pHmLength, boolean pMemoryEngine) {
        this(pClazz, pTableName, pTableKey, pHmLength);
        if (pMemoryEngine) {
            this.createSQL = this.createSQL + " ENGINE MEMORY";
        }
    }

    @Override
    public String getTableName() {
        return this.TABLENAME;
    }

    @Override
    public boolean dropTable() {
        String drop = "drop table if exists " + TABLENAME;
        try (Connection conn = DBTool.getMainConn(true)) {
            PreparedStatement ps = conn.prepareStatement(drop);
            ps.execute();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * No excpetion, success return true. fail false.
     *
     * @param o the object whose primary key value is going to be used to
     * delete.
     * @return
     */
    @Override
    public boolean delByPriKey(T o) {
        try (Connection conn = DBTool.getMainConn(true)) {
            PreparedStatement ps = conn.prepareStatement(delPkSQL);
            for (int i = 0; i < this.TABLEKEY.length; i++) {
                Object value = this.clazz.getDeclaredField(this.TABLEKEY[i]).get(o);
                if (value != null) {
                    ps.setObject(i + 1, this.clazz.getDeclaredField(this.TABLEKEY[i]).get(o));
                } else {
                    throw new java.sql.SQLDataException("Primary key cannot be null when delByPriKey" + o);
                }
            }
            ps.executeUpdate();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean deleteFromTable() {
        String del = "delete from " + TABLENAME;
        try (Connection conn = DBTool.getMainConn(true)) {
            PreparedStatement ps = conn.prepareStatement(del);
            ps.execute();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private String genFieldSqlDef(Field pF) {
        Class type = pF.getType();
        if (type.equals(double.class) || type.equals(Double.class)) {
            return pF.getName() + " " + "double ";
        } else if (type.equals(long.class) || type.equals(Long.class)) {
            return pF.getName() + " " + "bigint ";
        } else if (type.equals(boolean.class) || type.equals(Boolean.class)) {
            return pF.getName() + " " + "boolean ";
        } else if (type.equals(String.class)) {
            String sname = pF.getName();
            int l = sname.length();

            if (hmLength.get(sname) != null && hmLength.get(sname) > 0) {
                if (hmLength.get(sname) > 0 && hmLength.get(sname) <= MysqlAgent.STRINGDEFAULTLENGTH) {
                    return sname + " char(" + hmLength.get(sname) + ")";
                } else if (hmLength.get(sname) > MysqlAgent.STRINGDEFAULTLENGTH) {
                    return sname + " varchar(" + hmLength.get(sname) + ")";
                }
            } else if (l > 4 && sname.substring(l - 4, l).equalsIgnoreCase("date")) {
                hmLength.put(sname, 8);
                return sname + " char(8)";
            } else if (l > 4 && sname.substring(l - 3, l).matches("\\d+")) {
                int i = Integer.valueOf(sname.substring(l - 3, l));
                if (i <= 20) {
                    hmLength.put(sname, i);
                    return sname + " char(" + i + ")";
                } else if (i > 20) {
                    hmLength.put(sname, i);
                    return sname + "varchar(" + i + ")";
                }
            } else if (l > 4 && sname.substring(l - 4, l).equalsIgnoreCase("year")) {
                hmLength.put(sname, 4);
                return sname + " char(4)";
            } else {
                hmLength.put(sname, MysqlAgent.STRINGDEFAULTLENGTH);
                return sname + " varchar(" + MysqlAgent.STRINGDEFAULTLENGTH + ")";
            }
        }
        System.out.println(pF.getName() + " Field canonicalname: " + type.getCanonicalName() + " equal to double class?: " + (type.equals(Double.class) && type.equals(double.class)));
        System.out.println(pF.getName() + " Field canonicalname: " + type.getCanonicalName() + " equal to long class?: " + (type.equals(Long.class) && type.equals(long.class)));
        System.out.println(pF.getName() + " Field canonicalname: " + type.getCanonicalName() + " equal to String.class?: " + type.equals(String.class));
        logger.error("FATAL: Cannot generate field sql definition.Pojo class: " + this.clazz.getCanonicalName());
        try {
            throw new Exception();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    @Override
    public String genSelectClause() {
        StringBuilder builder = new StringBuilder("select ");
        for (int j = 0; j < pojoFields.length - 1; j++) {
            builder.append(pojoFields[j].getName() + ", ");
        }
        builder.append(pojoFields[pojoFields.length - 1].getName()).append(" from ").append(this.TABLENAME);
        return builder.toString();
    }

    @Override
    public List<T> selectAll() {
        List<T> result = new ArrayList();
//        String sqlSelect = SymbolUS.pojo.genSelectClause() + " where " + " today = ? and market = ? limit 0, ?";
        String sqlSelect = this.genSelectClause(); //+ " where " + " today = ? limit 0, ?";
        try (Connection conn = DBTool.getMainConn(true)) {
            PreparedStatement ps = conn.prepareStatement(sqlSelect);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                T object = null;
                try {
                    Constructor constructor = clazz.getConstructor(new Class[]{});
                    if (constructor != null) {
                        object = clazz.newInstance();
                    } else {
                        throw new Exception("Constructor for " + clazz.getCanonicalName() + " with no parameter  not found");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
                for (int j = 0; j < this.pojoFields.length; j++) {
                    Field field = this.pojoFields[j];
                    Object value = rs.getObject(j + 1);
                    if (rs.wasNull() ){
                        field.set(object, null);
                    } else {
                        field.set(object, value);
                    }
                }
                result.add(object);
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private String genCreateSQL() {
        StringBuilder builder = new StringBuilder("create table if not exists " + this.TABLENAME + " (");
        boolean keysfound = true;
        for (int i = 0; i < this.TABLEKEY.length; i++) {
            boolean keyfound = false;
            for (int j = 0; j < pojoFields.length; j++) {
                if (pojoFields[j].getName().equals(this.TABLEKEY[i])) {
                    keyfound = true;
                    break;
                }
            }
            if (!keyfound) {
                keysfound = false;
                break;
            }
        }
        if (!keysfound) {
            logger.error("FATAL: Table key not found in class properties for table: " + this.TABLENAME + ". for class: " + this.clazz.getCanonicalName());
            System.exit(-1);
        }
        for (int i = 0; i < pojoFields.length; i++) {
            builder.append(genFieldSqlDef(pojoFields[i]) + ", ");
        }
        builder.append("primary key (");
        for (int i = 0; i < this.TABLEKEY.length - 1; i++) {
            builder.append(this.TABLEKEY[i] + ",");
        }
        builder.append(this.TABLEKEY[this.TABLEKEY.length - 1] + ")) character set utf8");
        return builder.toString();
    }

    private String genDelPkSQL() {
        StringBuilder builder = new StringBuilder("delete from " + this.TABLENAME + " where ");
        for (int i = 0; i < this.TABLEKEY.length - 1; i++) {
            builder.append(this.TABLEKEY[i] + " = ? and ");
        }
        builder.append(this.TABLEKEY[this.TABLEKEY.length - 1] + " = ?");
        return builder.toString();
    }

    private String genReplaceSQL() {
        StringBuilder builder = new StringBuilder("replace into " + this.TABLENAME + "(");
        for (int i = 0; i < this.pojoFields.length - 1; i++) {
            builder.append(pojoFields[i].getName() + ", ");
        }
        builder.append(pojoFields[pojoFields.length - 1].getName() + ") values (");
        for (int i = 0; i < this.pojoFields.length - 1; i++) {
            builder.append("?, ");
        }
        builder.append("?)");
        return builder.toString();
    }

    @Override
    public boolean createTable() {
        try (Connection conn = DBTool.getMainConn(true)) {
            PreparedStatement ps = conn.prepareStatement(this.createSQL);
            ps.execute();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void replaceToDB(T o) {
        try (Connection conn = DBTool.getMainConn(true)) {
            PreparedStatement ps = conn.prepareStatement(this.replaceSQL);
            for (int i = 0; i < this.pojoFields.length; i++) {
                Field f = this.pojoFields[i];
                if ((f.getType().equals(Double.TYPE) || f.getType().equals(Double.class)) && (f.getDouble(o) == Double.POSITIVE_INFINITY)) {
                    ps.setObject(i + 1, Double.MAX_VALUE);
                } else {
                    ps.setObject(i + 1, f.get(o));
                }
            }
            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void replaceToDB(ArrayList<T> a) {
        try (Connection conn = DBTool.getMainConn(true)) {
            PreparedStatement ps = conn.prepareStatement(this.replaceSQL);
            for (int i = 0; i < a.size(); i++) {
                for (int i0 = 0; i0 < this.pojoFields.length; i0++) {
                    Field f = this.pojoFields[i0];
                    ps.setObject(i0 + 1, f.get(a.get(i)));
                }
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("SQL statement that leads to exception: " + this.replaceSQL);
        }
    }
}
