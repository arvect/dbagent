/*
 * Copyright 2016 oefish.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dongjindu.dbagent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.slf4j.LoggerFactory;

/**
 *
 * @author oefish
 */
public class IdAgent {

    private static org.slf4j.Logger logger = LoggerFactory.getLogger(IdAgent.class);
    private Class clazz;
    private DBAgent agent;

    public final static long DEFAULTINI = -1l;
    private long initialId = IdAgent.DEFAULTINI;
    private boolean iniChangeAllow = false;

    public long currId;

    public IdAgent(Class pClazz, DBAgent pAgent) {
        agent = pAgent;
        clazz = pClazz;
        try (Connection conn = DBTool.getMainConn(true)) {
            PreparedStatement ps = conn.prepareStatement("select max(id) from " + agent.getTableName());
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                if (rs.wasNull()) {
                    currId = initialId;
                } else {
                    currId = rs.getLong(1);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("failed to get currId, return Initial id: " + initialId);
            System.exit(-1);
        }
    }

    public boolean getIniChangeAllow() {
        return iniChangeAllow;
    }

    public void setIniChangeAllow(boolean pStatus) {
        iniChangeAllow = pStatus;
    }

    public void setInitialId(long pId) {
        if (iniChangeAllow) {
            initialId = pId;
        } else {
            logger.error("Failed to set initialId for " + clazz.getCanonicalName() + ". Try call setIniChangeAllow(true) first.");
        }
    }

    public long getInitialId() {
        return initialId;
    }

    public synchronized long getNextId() {
            currId = currId + 1;
            return currId;
    }
}
