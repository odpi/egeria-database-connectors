/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc;

import org.odpi.openmetadata.frameworks.auditlog.messagesets.ExceptionMessageDefinition;
import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;

/**
 * Exception to be thrown by the connector and processed in the IntegrationConnectorHandler
 */
public class JdbcConnectorException extends ConnectorCheckedException {

    public JdbcConnectorException(ExceptionMessageDefinition messageDefinition, String name, String methodName, Exception error) {
        super(messageDefinition, name, methodName, error);
    }
}
