/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.databasepollingrepository;

import java.util.Objects;
/**
 * This is a representation of a Column for the connector without any reference to the technology column representation.
 */
public class ConnectorColumn {
    String name;
    String qualifiedName;
    String type;

    public ConnectorColumn() {}
    public ConnectorColumn(String name, String qualifiedName, String type) {
        this.name= name;
        this.qualifiedName=qualifiedName;
        this.type=type;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorColumn that = (ConnectorColumn) o;
        return Objects.equals(name, that.name) && Objects.equals(qualifiedName, that.qualifiedName) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, qualifiedName, type);
    }


}
