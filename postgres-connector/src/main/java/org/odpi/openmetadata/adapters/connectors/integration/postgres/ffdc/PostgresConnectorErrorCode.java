package org.odpi.openmetadata.adapters.connectors.integration.postgres.ffdc;

import org.odpi.openmetadata.frameworks.auditlog.messagesets.ExceptionMessageDefinition;
import org.odpi.openmetadata.frameworks.auditlog.messagesets.ExceptionMessageSet;

/**
 * The KafkaOpenMetadataTopicConnectorErrorCode is used to define first failure data capture (FFDC) for errors that occur when working with
 * the Apache Kafka connector.  It is used in conjunction with both Checked and Runtime (unchecked) exceptions.
 *
 * The 5 fields in the enum are:
 * <ul>
 *     <li>HTTP Error Code - for translating between REST and JAVA - Typically the numbers used are:</li>
 *     <li><ul>
 *         <li>500 - internal error</li>
 *         <li>400 - invalid parameters</li>
 *         <li>404 - not found</li>
 *         <li>409 - data conflict errors - eg item already defined</li>
 *     </ul></li>
 *     <li>Error Message Id - to uniquely identify the message</li>
 *     <li>Error Message Text - includes placeholder to allow additional values to be captured</li>
 *     <li>SystemAction - describes the result of the error</li>
 *     <li>UserAction - describes how a consumer should correct the error</li>
 * </ul>
 */

public enum PostgresConnectorErrorCode implements ExceptionMessageSet
{
        ERROR_READING_DATABASES(400, "POSTGRES-CONNECTOR-400-001",
                "An unexpected {0} exception was caught while sending an event to topic {1}.  The message in the exception was: {2}",
                "The system is unable to send the event.",
                "Review the exception that was returned from the send."),

    ERROR_READING_SCHEMAS(400, "POSTGRES-CONNECTOR-400-002 ",
                "Egeria was unable to initialize a connection to a Kafka cluster.  The message in the exception was: {2}",
                "The system is unable initialize.",
                "Ensure that Kafka is available"),

    ERROR_ADDING_SCHEMAS( 400, "POSTGRES-CONNECTOR-400-003",
            "Adding a database schema to Egeria caused an invalid parameter error",
            "Ensure Egeria is available",
            "Ensure Egeria is available"),

    USER_NOT_AUTHORIZED( 400, "POSTGRES-CONNECTOR-400-004",
            "The user is not authorized to perform action",
            "Review the user access privlidges",
            "Review the user access privlidges"),


    INVALID_PROPERTY( 400, "POSTGRES-CONNECTOR-400-005",
            "An invalid property was passed to Egeria",
            "Review the property values passed to Egeria",
            "Review the property values passed to Egeria"),

    ERROR_READING_TABLES( 400, "POSTGRES-CONNECTOR-400-006",
            "An Exception was encountered reading the datbase tables",
            "Ensure that the Postgres database is reachable",
            "Ensure that the Postgres database is reachable"),

    INVALID_PARAMETER( 400, "POSTGRES-CONNECTOR-400-007",
            "The Method {1} received an Invalid parameter exception from the OMAS server",
            "Review the paramaters passed to Egeria",
            "Review the paramaters passed to Egeria"),

    UNEXPECTED_ERROR( 400, "POSTGRES-CONNECTOR-400-008",
            "The Postgres Connector experienced an unexpected error.",
            "Review the system logs to identify and resolve the issue.",
            "Review the system logs to identify and resolve the issue."),

    ERROR_READING_VIEWS( 400, "POSTGRES-CONNECTOR-400-009",
            "The Postgres Connector experienced an exception accessing the Postgres views.",
            "Ensure that the Postgres database server is available.",
            "Verify that the Postgres database server is available."),

    ERROR_READING_FOREIGN_KEYS( 400, "POSTGRES-CONNECTOR-400-010",
            "The Postgres Connector experienced an exception accessing the Postgres foreign keys.",
            "Ensure that the Postgres database server is available.",
            "Verify that the Postgres database server is available."),

    ERROR_READING_COLUMNS( 400, "POSTGRES-CONNECTOR-400-011",
            "The Postgres Connector experienced an exception accessing the Postgres columns.",
            "Ensure that the Postgres database server is available.",
            "Verify that the Postgres database server is available."),

    ERROR_REMOVING_COLUMNS( 400, "POSTGRES-CONNECTOR-400-011",
            "The Postgres Connector experienced an exception while trying to remove a database entity from Egeria.",
            "Ensure that the Postgres database server is available.",
            "Verify that the Postgres database server is available."),


    ERROR_REMOVING_DATABASES( 400, "POSTGRES-CONNECTOR-400-011",
            "The Postgres Connector experienced an exception while trying to remove a database entity from Egeria.",
            "Ensure that the Postgres database server is available.",
            "Verify that the Postgres database server is available."),

    PROPERTY_SERVER_EXCEPTION( 400, "POSTGRES-CONNECTOR-400-012",
            "The Egeria OMAS Server returned a Property Server Exception",
            "Verify that the Egeria OMAS server is available",
            "Verify that the Postgres database server is available."),

    CONNECTOR_CHECKED( 400, "POSTGRES-CONNECTOR-400-013",
            "The Egeria OMAS Server returned a Connector Exception",
            "Verify that the Egeria OMAS server is available",
            "Check the Exception details to identify the issue"),

    ALREADY_HANDLED_EXCEPTION( 400, "POSTGRES-CONNECTOR-400-014",
            "Passing a handled exception to the connector",
            "Check the nested exception for root cause",
            "Check the nested exception user action for resolution"),

    ;

    private final ExceptionMessageDefinition messageDefinition;


        /**
         * The constructor for KafkaOpenMetadataTopicConnectorErrorCode expects to be passed one of the enumeration rows defined in
         * KafkaOpenMetadataTopicConnectorErrorCode above.   For example:
         *
         *     KafkaOpenMetadataTopicConnectorErrorCode   errorCode = KafkaOpenMetadataTopicConnectorErrorCode.ERROR_SENDING_EVENT;
         *
         * This will expand out to the 5 parameters shown below.
         *
         *
         * @param httpErrorCode   error code to use over REST calls
         * @param errorMessageId   unique Id for the message
         * @param errorMessage   text for the message
         * @param systemAction   description of the action taken by the system when the error condition happened
         * @param userAction   instructions for resolving the error
         */
        PostgresConnectorErrorCode(int  httpErrorCode, String errorMessageId, String errorMessage, String systemAction, String userAction)
        {
            this.messageDefinition = new ExceptionMessageDefinition(httpErrorCode,
                    errorMessageId,
                    errorMessage,
                    systemAction,
                    userAction);
        }


        /**
         * Retrieve a message definition object for an exception.  This method is used when there are no message inserts.
         *
         * @return message definition object.
         */
        @Override
        public ExceptionMessageDefinition getMessageDefinition()
        {
            return messageDefinition;
        }


        /**
         * Retrieve a message definition object for an exception.  This method is used when there are values to be inserted into the message.
         *
         * @param params array of parameters (all strings).  They are inserted into the message according to the numbering in the message text.
         * @return message definition object.
         */
        @Override
        public ExceptionMessageDefinition getMessageDefinition(String... params)
        {
            messageDefinition.setMessageParameters(params);

            return messageDefinition;
        }
}


