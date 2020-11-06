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

    EVENT_SEND_IN_ERROR_LOOP("OCF-KAFKA-TOPIC-CONNECTOR-0012",
            OMRSAuditLogRecordSeverity.ERROR,
            "Unable to send event on topic {0}.  {1} events successfully sent; {2} events buffered. Latest error message is {3}",
            "There is a reoccurring error being returned by the Apache Kafka event bus.  Outbound events are being buffered.",
            "Review the operational status of Apache Kafka to ensure it is running and the topic is defined.  " +
                    "If no events have been send, then it may be a configuration error, either in this " +
                    "server or in the event bus itself. Once the error is corrected, " +
                    "the server will send the buffered events.  "),

    MISSING_PROPERTY( "OCF-KAFKA-TOPIC-CONNECTOR-0013 ",
            OMRSAuditLogRecordSeverity.ERROR,
            "Property {0} is missing from the Kafka Event Bus configuration",
            "The system is unable to connect to the event bus.",
            "Add the missing property to the event bus properties in the server configuration."),

    SERVICE_FAILED_INITIALIZING( "OCF-KAFKA-TOPIC-CONNECTOR-0014 ",
            OMRSAuditLogRecordSeverity.ERROR,
            "Connecting to bootstrap Apache Kafka Broker {0}",
            "The local server has failed to started up the Apache Kafka connector, Kafka Broker is unavailable",
            "Ensure Kafka is running and restart the local Egeria Server"),

    KAFKA_CONNECTION_RETRY( "OCF-KAFKA-TOPIC-CONNECTOR-0015",
            OMRSAuditLogRecordSeverity.STARTUP,
            "The local server is attempting to connect to Kafka, attempt {0}",
            "The system retries the connection after a short wait.",
            "Ensure the Kafka Cluster has started"),
    UNEXPECTED_SHUTDOWN_EXCEPTION( "OCF-KAFKA-TOPIC-CONNECTOR-0016",
            OMRSAuditLogRecordSeverity.SHUTDOWN,
            "An unexpected error {0} was encountered while closing the kafka topic connector for {1}: action {2} and error message {3}",
            "The connector continues to shutdown.  Some resources may not be released properly.",
            "Check the OMAG Server's audit log and Kafka error logs for related messages that may indicate " +
                    "if there are any unreleased resources."),
    EXCEPTION_COMMITTING_OFFSETS("OCF-KAFKA-TOPIC-CONNECTOR-0017",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "An unexpected error {0} was encountered while committing consumed event offsets to topic {1}: error message is {2}",
            "Depending on the nature of the error, events may no longer be exchanged with the topic.",
            "Check the OMAG Server's audit log and Kafka error logs for related messages that " +
                    "indicate the cause of this error.  Work to clear the underlying error.  " +
                    "Once fixed, it may be necessary to restart the server to cause a reconnect to Kafka.")
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
    public AuditLogMessageDefinition getMessageDefinition(String ...params)
    {
        messageDefinition.setMessageParameters(params);
        return messageDefinition;
    }
}
