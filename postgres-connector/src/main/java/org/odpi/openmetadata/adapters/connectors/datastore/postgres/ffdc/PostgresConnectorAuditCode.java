/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.datastore.postgres.ffdc;
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
    ERROR_READING_DATABASES("POSTGRES-CONNECTOR-0001",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "Error Reading Databases from Server",
            "Verify that the Database Server is available",
            "Verify that the connector is being passed the correct connection properties"),

    ERROR_READING_SCHEMAS("POSTGRES-CONNECTOR-0002",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "Error Reading Schemas for Database {1}",
            "The server is registering to receive events from Apache Kafka using the properties associated with this log record.",
            "No action is required.  This is part of the normal operation of the server."),

    ERROR_ADDING_SCHEMAS("POSTGRES-CONNECTOR-0003",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "{0} properties passed to the Apache Kafka Consumer for topic {1}",
            "The server is registering to receive events from Apache Kafka using the properties associated with this log record.",
            "No action is required.  This is part of the normal operation of the server."),

    USER_NOT_AUTORIZED("POSTGRES-CONNECTOR-0004",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "The user is not autoized to perform that operation",
            "Operation refused",
            "Review the user's privileges"),

    INVALID_PARAMETER("POSTGRES-CONNECTOR-0005",
            OMRSAuditLogRecordSeverity.ERROR,
            "An invalid paramter was passed to Egeria.",
            "The request has been rejected.",
            "This problem must be fixed before the Postgres Connector can exchange metadata."),

    INVALID_PROPERTY("POSTGRES-CONNECTOR-0006",
            OMRSAuditLogRecordSeverity.ERROR,
            "An invalid property was passed to Egeria",
            "Egeria was passed an invalid property",
            "This problem must be fixed before the server can exchange metadata."),

    ERROR_READING_TABLES("POSTGRES-CONNECTOR-0007",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "The Postgres Connector experienced an error reading the database tables.",
            "Ensure that the source Postgres database is available.",
            "Verify that the source Postgres database is available."),

    UNEXPECTTED_ERROR("POSTGRES-CONNECTOR-0008",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "The Postgres Connector experienced an unexpected exception.",
            "Examine the system logs to identify the issue.",
            "Use the information in the event and the exception message, along with other messages to determine the source of the error."),

    ERROR_READING_VIEWS("POSTGRES-CONNECTOR-0009",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "The Postgres Connector experienced an error reading the database views.",
            "Ensure that the source Postgres database is available.",
            "Verify that the source Postgres database is available."),

    ERROR_READING_FOREGIN_KEYS("POSTGRES-CONNECTOR-0010",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "The Postgres Connector experienced an error reading the database foregin keys.",
            "Ensure that the source Postgres database is available.",
            "Verify that the source Postgres database is available."),

    ERROR_READING_COLUMNS("POSTGRES-CONNECTOR-0011",
            OMRSAuditLogRecordSeverity.SHUTDOWN,
            "The Postgres Connector experienced a problem reading the table columns.",
            "Ensure that the Postgres database server is available.",
            "Ensure that the Postgres database server is available."),

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
