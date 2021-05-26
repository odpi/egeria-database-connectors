package org.odpi.openmetadata.adapters.connectors.integration.postgres.ffdc;

import org.odpi.openmetadata.frameworks.auditlog.messagesets.ExceptionMessageDefinition;
import org.odpi.openmetadata.frameworks.auditlog.messagesets.ExceptionMessageSet;

/**
 * The PostgresConnectorErrorCode is used to define first failure data capture (FFDC) for errors that occur when working with
 * the Postgres connector.  It is used in conjunction with both Checked and Runtime (unchecked) exceptions.
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

    ERROR_READING_FROM_POSTGRES(400, "POSTGRES-CONNECTOR-400-001",
                "An exception was caught by method {0} while while trying to read from the postgres server.",
                "Verify that the connector can connect to the postgres server",
                "Enable connection to the postgres server and restart the connector"),

    USER_NOT_AUTHORIZED_EXCEPTION( 400, "POSTGRES-CONNECTOR-400-002",
            "The user is not authorized to perform action",
            "Review the user access privlidges",
            "Review the user access privlidges"),

    INVALID_PARAMETER_EXCEPTION( 400, "POSTGRES-CONNECTOR-400-003",
            "The Method {0} received an Invalid parameter exception from the OMAS server.",
            "Review the paramaters passed to Egeria ",
            "Review the paramaters passed to Egeria"),

    UNEXPECTED_ERROR( 400, "POSTGRES-CONNECTOR-400-004",
            "The Postgres Connector experienced an unexpected error in method (0).",
            "Review the system logs to identify and resolve the issue.",
            "Review the system logs to identify and resolve the issue."),


    PROPERTY_SERVER_EXCEPTION( 400, "POSTGRES-CONNECTOR-400-005",
            "The Egeria OMAS Server returned a Property Server Exception to method {0}",
            "Verify that the Egeria OMAS server is available",
            "Verify that the Postgres database server is available."),

    CONNECTOR_CHECKED_EXCEPTION( 400, "POSTGRES-CONNECTOR-400-006",
            "The Egeria OMAS Server returned a Connector Exception to method {0}",
            "Verify that the Egeria OMAS server is available",
            "Check the Exception details to identify the issue"),

    ALREADY_HANDLED_EXCEPTION( 400, "POSTGRES-CONNECTOR-400-007",
            "Passing a handled exception to the connector",
            "Check the nested exception for root cause",
            "Check the nested exception user action for resolution"),

    ;

    private final ExceptionMessageDefinition messageDefinition;


        /**
         * The constructor for PostgresConnectorErrorCode expects to be passed one of the enumeration rows defined in
         * PostgresConnectorErrorCode above.   For example:
         *
         *     PostgresConnectorErrorCode   errorCode = PostgresConnectorErrorCode.ERROR_SENDING_EVENT;
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


