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

    SQL_EXCEPTION_ON_CONNECTION_CREATE("JDBC-CONNECTOR-0001",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "An sql exception was received by method {0}. Exception message is {1}",
            "Connecting to database server",
            "Investigate database server availability. If the database server is available then contact the Egeria team for support"),
    EXITING_ON_CONNECTION_FAIL("JDBC-CONNECTOR-0002",
            OMRSAuditLogRecordSeverity.INFO,
            "Exiting from method {0} as a result of a failed connection",
            "Stopping execution",
            "Investigate database server availability. If the database server is available then contact the Egeria team for support"),
    SQL_EXCEPTION_ON_CONNECTION_CLOSE("JDBC-CONNECTOR-0003",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "An sql exception was received by method {0}. Exception message is {1}",
            "Closing connection to database server",
            "Investigate database server availability. If the database server is available then contact the Egeria team for support"),
    EXITING_ON_COMPLETE("JDBC-CONNECTOR-0004",
            OMRSAuditLogRecordSeverity.INFO,
            "Execution of method {0} is complete",
            "Stopping execution",
            "No user actions necessary"),
    EXITING_ON_TRANSFER_FAIL("JDBC-CONNECTOR-0005",
            OMRSAuditLogRecordSeverity.INFO,
            "Execution of method {0} is complete, however it finished abnormally",
            "Stopping execution",
            "Consult logs for further details"),
    EXITING_ON_INTEGRATION_CONTEXT_FAIL("JDBC-CONNECTOR-0006",
            OMRSAuditLogRecordSeverity.INFO,
            "Retrieving integration context failed in method {0}",
            "Stopping execution",
            "Consult logs for further details"),
    ERROR_READING_JDBC("JDBC-CONNECTOR-0007",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "An sql exception was received by method {0}. Exception message is {1}",
            "Reading JDBC",
            "Investigate database server availability. If the database server is available then contact the Egeria team for support"),
    ERROR_UPSERTING_INTO_OMAS("JDBC-CONNECTOR-0008",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "An exception was received by method {0}. Exception message is {1}",
            "Upserting an entity into omas failed.",
            "Investigate OMAS availability. If it is available then contact the Egeria team for support"),
    EXITING_ON_METADATA_TRANSFER("JDBC-CONNECTOR-0009",
            OMRSAuditLogRecordSeverity.INFO,
            "Transferring metadata failed in method {0}",
            "Stopping execution",
            "Consult logs for further details"),
    ERROR_READING_OMAS("JDBC-CONNECTOR-0010",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "Error reading data from omas in method {0}",
            "Reading omas information",
            "Consult logs for further details"),
    UNKNOWN_ERROR_WHILE_METADATA_TRANSFER("JDBC-CONNECTOR-0011",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "Unknown error transferring metadata in method {0}.",
            "Reading metadata information",
            "Consult logs for further details"),


    USER_NOT_AUTHORIZED_EXCEPTION("JDBC-CONNECTOR-0002",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "The method {0} generated a UserNotAuthorized. {1} {2} ",
            "Operation refused",
            "Review the user's privileges"),
    PROPERTY_SERVER_EXCEPTION("POSTGRES-CONNECTOR-0003",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "The call from method {0} generated a PropertyServerException from the OMAS server",
            "Operation refused",
            "Correct the property that is causing the error"),

    INVALID_PARAMETER_EXCEPTION("POSTGRES-CONNECTOR-0004",
            OMRSAuditLogRecordSeverity.ERROR,
            "The Method {0} generated an InvalidParameterException from the OMAS server. {1} {2}",
            "The request has been rejected.",
            "This problem must be fixed before the Postgres Connector can exchange metadata."),


    CONNECTOR_CHECKED_EXCEPTION("POSTGRES-CONNECTOR-0005",
            OMRSAuditLogRecordSeverity.SHUTDOWN,
            "Method {0} received a connector checked exception from the OMAS server. {1} {2}",
            "Ensure that the OMAS server is availabale and is responsive",
            "Check exception details to rectify the problem"),

    UNEXPECTED_ERROR("POSTGRES-CONNECTOR-0006",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "The method {0} encountered an unexpected error. {1} {2}",
            "Examine the system logs to identify the issue.",
            "Use the information in the event and the exception message, along with other messages to determine the source of the error."),

    ;


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
