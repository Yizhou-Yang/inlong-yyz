/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.tchousex.flink.model;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * table data for a batch
 * We check insertData when rowkind is delete. We can remove the row data when insertData contains delete data.
 * We check insertData when rowkind is update_after. We can remove the row data when insertData contains update_after
 * data. And We can insertData add update_after data.
 */
public class TCHouseXData {

    // key is identificationData
    private Map<String, List<String>> insertData;
    private List<List<String>> updateData;
    private List<List<String>> deleteData;
    private List<RowData> rowDataList;
    private RowType rowType;
    private AtomicInteger updateBeforeRecordNum;

    public TCHouseXData() {
        insertData = new HashMap<>();
        updateData = new ArrayList<>();
        deleteData = new ArrayList<>();
        rowDataList = new ArrayList<>();
        updateBeforeRecordNum = new AtomicInteger(0);
    }

    public Map<String, List<String>> getInsertData() {
        return insertData;
    }

    public void setInsertData(Map<String, List<String>> insertData) {
        this.insertData = insertData;
    }

    public List<List<String>> getUpdateData() {
        return updateData;
    }

    public void setUpdateData(List<List<String>> updateData) {
        this.updateData = updateData;
    }

    public List<List<String>> getDeleteData() {
        return deleteData;
    }

    public void setDeleteData(List<List<String>> deleteData) {
        this.deleteData = deleteData;
    }

    public List<RowData> getRowDataList() {
        return rowDataList;
    }

    public void setRowDataList(List<RowData> rowDataList) {
        this.rowDataList = rowDataList;
    }

    public RowType getRowType() {
        return rowType;
    }

    public void setRowType(RowType rowType) {
        this.rowType = rowType;
    }

    public AtomicInteger getUpdateBeforeRecordNum() {
        return updateBeforeRecordNum;
    }

    public void setUpdateBeforeRecordNum(AtomicInteger updateBeforeRecordNum) {
        this.updateBeforeRecordNum = updateBeforeRecordNum;
    }
}
