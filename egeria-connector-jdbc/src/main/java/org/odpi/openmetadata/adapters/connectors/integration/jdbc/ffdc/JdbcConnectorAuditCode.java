/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc;
import org.odpi.openmetadata.frameworks.auditlog.messagesets.AuditLogMessageDefinition;
import org.odpi.openmetadata.frameworks.auditlog.messagesets.AuditLogMessageSet;
import org.odpi.openmetadata.repositoryservices.auditlog.OMRSAuditLogRecordSeverity;


/**
 * The JdbcConnectorAuditCode is used to define the message content for the Audit Log.
 *
 * The 5 fields in the enum are:
 * <ul>
 *     <li>Log Message Id - to uniquely identify the message</li>
 *     <li>Severity - is this an event, decision, action, error or exception</li>
 *     <li>Log Message Text - includes placeholder to allow additional values to be captured</li>
 *     <li>SystemAction - describes the result of the situation</li>
 *     <li>UserAction - describes how a user should correct the situation</li>
 * </ul>
 */
public enum JdbcConnectorAuditCode implements AuditLogMessageSet {

    EXITING_ON_CONNECTION_FAIL("JDBC-CONNECTOR-0001",
            OMRSAuditLogRecordSeverity.INFO,
            "Exiting from method {0} as a result of a failed connection",
            "Stopping execution",
            "Investigate database server availability. If the database server is available then contact the Egeria team for support"),
    EXITING_ON_COMPLETE("JDBC-CONNECTOR-0002",
            OMRSAuditLogRecordSeverity.INFO,
            "Execution of method {0} is complete",
            "Stopping execution",
            "No user actions necessary"),
    EXITING_ON_TRANSFER_FAIL("JDBC-CONNECTOR-0003",
            OMRSAuditLogRecordSeverity.INFO,
            "Execution of method {0} is complete, however it finished abnormally",
            "Stopping execution",
            "Consult logs for further details"),
    EXITING_ON_INTEGRATION_CONTEXT_FAIL("JDBC-CONNECTOR-0004",
            OMRSAuditLogRecordSeverity.INFO,
            "Retrieving integration context failed in method {0}",
            "Stopping execution",
            "Consult logs for further details"),
    ERROR_READING_JDBC("JDBC-CONNECTOR-0005",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "An SQL exception was received by method {0}. Exception message is: {1}",
            "Reading JDBC",
            "Investigate database server availability. If the database server is available then contact the Egeria team for support"),
    ERROR_UPSERTING_INTO_OMAS("JDBC-CONNECTOR-0006",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "An exception was received by method {0}. Exception message is: {1}",
            "Upserting an entity into omas failed.",
            "Investigate OMAS availability. If it is available then contact the Egeria team for support"),
    EXITING_ON_METADATA_TRANSFER("JDBC-CONNECTOR-0007",
            OMRSAuditLogRecordSeverity.INFO,
            "Transferring metadata failed in method {0}",
            "Stopping execution",
            "Consult logs for further details"),
    ERROR_READING_OMAS("JDBC-CONNECTOR-0008",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "Error reading data from omas in method {0}. Possible message is {1}",
            "Reading omas information",
            "Consult logs for further details"),
    UNKNOWN_ERROR_WHILE_METADATA_TRANSFER("JDBC-CONNECTOR-0009",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "Unknown error transferring metadata in method {0}.",
            "Reading metadata information",
            "Consult logs for further details"),
    ERROR_WHEN_REMOVING_ELEMENT_IN_OMAS("JDBC-CONNECTOR-0010",
            OMRSAuditLogRecordSeverity.INFO,
            "Unknown error when removing element in omas with guid {0} and qualified name {1}.",
            "Removing element in omas",
            "Consult logs for further details"),
    ERROR_WHEN_SETTING_ASSET_CONNECTION("JDBC-CONNECTOR-0011",
            OMRSAuditLogRecordSeverity.INFO,
            "Unknown error when setting up asset connection in method {0}.",
            "Setting up asset connection",
            "Consult logs for further details");


    private final AuditLogMessageDefinition messageDefinition;


    /**
     * The constructor for JdbcConnectorAuditCode expects to be passed one of the enumeration rows defined above.
     * Example:
     *
     * JdbcConnectorAuditCode auditCode = JdbcConnectorAuditCode.EXCEPTION_COMMITTING_OFFSETS;
     *
     * This will expand out to the 4 parameters shown below.
     *
     * @param messageId unique Id for the message
     * @param severity severity of the message
     * @param message text for the message
     * @param systemAction description of the action taken by the system when the condition happened
     * @param userAction instructions for resolving the situation, if any
     */
    JdbcConnectorAuditCode(String messageId, OMRSAuditLogRecordSeverity severity, String message, String systemAction,
                           String userAction){
        messageDefinition = new AuditLogMessageDefinition(messageId, severity, message, systemAction, userAction);
    }


    /**
     * Retrieve a message definition object for logging.  This method is used when there are no message inserts.
     *
     * @return message definition object.
     */
    @Override
    public AuditLogMessageDefinition getMessageDefinition() {
        return messageDefinition;
    }


    /**
     * Retrieve a message definition object for logging.  This method is used when there are values to be inserted into the message.
     *
     * @param params array of parameters (all strings). They are inserted into the message according to the numbering in the message text.
     *
     * @return message definition object.
     */
    @Override
    public AuditLogMessageDefinition getMessageDefinition(String... params) {
        messageDefinition.setMessageParameters(params);
        return messageDefinition;
    }
}
