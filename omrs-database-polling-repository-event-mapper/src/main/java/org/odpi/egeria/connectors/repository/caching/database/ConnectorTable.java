/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.repository.caching.database;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * This is a representation of a Table for the connector without any reference to the technology table representation.
 */
public class ConnectorTable {
    String name;
    String qualifiedName;
    String type;
    Date createTime;
    String hmsViewOriginalText;

    List<ConnectorColumn> columns;

    public ConnectorTable() {
    }

    public ConnectorTable(String name, String qualifiedName, String type, Date createTime, String hmsViewText) {
        this.name = name;
        this.qualifiedName = qualifiedName;
        this.type = type;
        this.createTime = createTime;
        this.hmsViewOriginalText = hmsViewText;

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getQualifiedName() {
        return qualifiedName;
    }

    public void setQualifiedName(String qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getHmsViewOriginalText() {
        return hmsViewOriginalText;
    }

    public void setHmsViewOriginalText(String hmsViewOriginalText) {
        this.hmsViewOriginalText = hmsViewOriginalText;
    }

    public List<ConnectorColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<ConnectorColumn> columns) {
        this.columns = columns;
    }

    public void addColumn(ConnectorColumn column) {
        if (columns == null) {
            columns = new ArrayList<>();
        }
        columns.add(column);
    }

    @Override
    @SuppressWarnings("JavaUtilDate")
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorTable that = (ConnectorTable) o;

        if (createTime == null) {
            if (that.createTime != null){
                return false;
            }
        } else {
            if ( that.createTime == null){
                return false;
            } else if (createTime.getTime() != that.createTime.getTime()) {
                return false;
            }
        }



        return Objects.equals(name, that.name) && Objects.equals(qualifiedName, that.qualifiedName) && Objects.equals(type, that.type) && Objects.equals(hmsViewOriginalText, that.hmsViewOriginalText) && Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, qualifiedName, type, createTime, hmsViewOriginalText, columns);
    }

    @Override
    public String toString() {
        return "ConnectorTable{" +
                "name='" + name + '\'' +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", type='" + type + '\'' +
                ", createTime=" + createTime +
                ", HMSViewOriginalText='" + hmsViewOriginalText + '\'' +
                ", columns=" + columns +
                '}';
    }
}
