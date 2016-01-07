/*
 * Copyright 2015 Dong Jin Du.
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
package dongjindu.dbagent;

import java.util.ArrayList;
import java.util.List;

public interface DBAgent<T> {
    int STRINGDEFAULTLENGTH = 30;

    String getTableName();
    boolean createTable();

    /**
     * No excpetion, success return true. fail false.
     *
     * @param o the object whose primary key value is going to be used to
     * delete.
     * @return
     */
    boolean delByPriKey(T o);

    boolean deleteFromTable();

    boolean dropTable();

    String genSelectClause();

    void replaceToDB(T o);

    void replaceToDB(ArrayList<T> a);

    List<T> selectAll();
    
}
