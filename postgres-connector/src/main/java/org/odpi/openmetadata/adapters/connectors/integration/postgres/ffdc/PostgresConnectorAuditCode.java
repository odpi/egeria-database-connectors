/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.postgres.ffdc;
import org.odpi.openmetadata.frameworks.auditlog.messagesets.AuditLogMessageDefinition;
import org.odpi.openmetadata.frameworks.auditlog.messagesets.AuditLogMessageSet;
import org.odpi.openmetadata.repositoryservices.auditlog.OMRSAuditLogRecordSeverity;


/**
 * The PostgresConnectorAuditCode is used to define the message content for the Audit Log.
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
public enum PostgresConnectorAuditCode implements AuditLogMessageSet
{
    ERROR_READING_POSTGRES("POSTGRES-CONNECTOR-0001",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "An SQL Exception was received by method {0} from the postgres server. The exception name {1} exception message {2}",
            "Verify that the postgres database is available",
            "If the postres database is available then contact the egeria team for support"),

    USER_NOT_AUTORIZED_EXCEPTION("POSTGRES-CONNECTOR-0002",
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

    UNEXPECTTED_ERROR("POSTGRES-CONNECTOR-0006",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "The method {0} encountered an unexpected error. {1} {2}",
            "Examine the system logs to identify the issue.",
            "Use the information in the event and the exception message, along with other messages to determine the source of the error."),

    ;


    private final AuditLogMessageDefinition messageDefinition;


    /**
     * The constructor for KafkaOpenMetadataTopicConnectorAuditCode expects to be passed one of the enumeration rows defined in
     * KafkaOpenMetadataTopicConnectorAuditCode above.   For example:
     *
     *     KafkaOpenMetadataTopicConnectorAuditCode   auditCode = KafkaOpenMetadataTopicConnectorAuditCode.EXCEPTION_COMMITTING_OFFSETS;
     *
     * This will expand out to the 4 parameters shown below.
     *
     * @param messageId unique Id for the message
     * @param severity severity of the message
     * @param message text for the message
     * @param systemAction description of the action taken by the system when the condition happened
     * @param userAction instructions for resolving the situation, if any
     */
    PostgresConnectorAuditCode(String                     messageId,
                                             OMRSAuditLogRecordSeverity severity,
                                             String                     message,
                                             String                     systemAction,
                                             String                     userAction)
    {
        messageDefinition = new AuditLogMessageDefinition(messageId,
                severity,
                message,
                systemAction,
                userAction);
    }


    /**
     * Retrieve a message definition object for logging.  This method is used when there are no message inserts.
     *
     * @return message definition object.
     */
    @Override
    public AuditLogMessageDefinition getMessageDefinition()
    {
        return messageDefinition;
    }


    /**
     * Retrieve a message definition object for logging.  This method is used when there are values to be inserted into the message.
     *
     * @param params array of parameters (all strings).  They are inserted into the message according to the numbering in the message text.
     * @return message definition object.
     */
    @Override
    public AuditLogMessageDefinition getMessageDefinition(String ...params)
    {
        messageDefinition.setMessageParameters(params);
        return messageDefinition;
    }
}
