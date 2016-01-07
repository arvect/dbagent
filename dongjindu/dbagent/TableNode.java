/*
 * Author: Dong Jin Du
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dongjindu.dbagent;

import java.util.ArrayList;

public class TableNode {

    public final ArrayList<TableNode> subTNodes;
    public String tableName;

    public TableNode(String pTableName) {
        tableName = pTableName;
        subTNodes = new ArrayList();
    }
}
