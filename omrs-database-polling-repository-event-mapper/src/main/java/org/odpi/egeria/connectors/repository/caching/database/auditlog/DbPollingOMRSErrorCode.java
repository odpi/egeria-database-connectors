/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.repository.caching.database.auditlog;

import org.odpi.openmetadata.frameworks.auditlog.messagesets.ExceptionMessageDefinition;
import org.odpi.openmetadata.frameworks.auditlog.messagesets.ExceptionMessageSet;

public enum DbPollingOMRSErrorCode implements ExceptionMessageSet {

    INVALID_CLASSIFICATION_FOR_ENTITY(400, "OMRS-HMS-REPOSITORY-400-006",
            "Sample file repository proxy is unable to assign a classification of type {0} to an entity of type {1} because the classification type is not valid for this type of entity",
            "The system is unable to classify an entity because the ClassificationDef for the classification does not list this entity type, or one of its super-types.",
            "Update the ClassificationDef to include the entity's type and rerun the request. Alternatively use a different classification."),
    INVALID_RELATIONSHIP_ENDS(400, "OMRS-HMS-REPOSITORY-400-047",
            "A {0} request has been made to repository {1} for a relationship that has one or more ends of the wrong or invalid type.  Relationship type is {2}; entity proxy for end 1 is {3} and entity proxy for end 2 is {4}",
            "The system is unable to perform the request because the instance has invalid values.",
            "Correct the caller's code and retry the request."),
    INVALID_INSTANCE(400, "OMRS-HMS-REPOSITORY-400-061",
            "An invalid instance has been detected by repository helper method {0}.  The instance is {1}",
            "The system is unable to work with the supplied instance because key values are missing from its contents.",
            "This is probably a logic error in the connector. Raise a git issue to get this investigated and fixed."),
    HOME_REFRESH(400, "OMRS-HMS-REPOSITORY-400-063",
            "Method {0} is unable to request a refresh of instance {1} as it is a local member of metadata collection {2} in repository {3}",
            "The system is unable to process the request.",
            "Review the error message and other diagnostics created at the same time."),
    EVENT_MAPPER_NOT_INITIALIZED(400, "OMRS-HMS-REPOSITORY-400-064 ",
                                 "There is no valid event mapper for repository \"{1}\"",
                                 "Appropriate event could not be produced for request",
                                 "Check the system logs and diagnose or report the problem."),
    EMBEDDED_IN_MEMORY_REPO_NOT_INITIALIZED(400, "OMRS-HMS-REPOSITORY-400-065 ",
                                 "The embedded in memory repository failed to initialise \"{1}\"",
                                 "Connector unable to start",
                                 "Check the system logs and diagnose or report the problem."),
    ENDPOINT_NOT_SUPPLIED_IN_CONFIG(400, "OMRS-HMS-REPOSITORY-400-066 ",
                                  "The endpoint was not supplied in the connector configuration \"{1}\"",
                                  "Connector unable to continue",
                                  "Supply a valid thrift end point in the configuration endpoint."),
    FAILED_TO_START_CONNECTOR(400, "OMRS-HMS-REPOSITORY-400-067 ",
                                   "The Hive metastore connector failed to start",
                                   "Connector is unable to be used",
                                   "Review your configuration to ensure it is valid."),

    INVALID_PARAMETER_EXCEPTION(400, "OMRS-HMS-REPOSITORY-400-073 ",
                               "Invalid parameter exception",
                               "Connector is unable to be used",
                               "Review the configuration. Check the logs and debug."),

    REPOSITORY_ERROR_EXCEPTION(400, "OMRS-HMS-REPOSITORY-400-074 ",
                                "Repository error excpption",
                                "Connector is unable to be used",
                                "Review the configuration. Check the logs and debug."),
    TYPE_ERROR_EXCEPTION(400, "OMRS-HMS-REPOSITORY-400-075 ",
                         "Type error exception",
                         "Connector is unable to be used",
                         "Review the configuration. Check the logs and debug."),
    PROPERTY_ERROR_EXCEPTION(400, "OMRS-HMS-REPOSITORY-400-076 ",
                                "Property error exception",
                                "Connector is unable to be used",
                                "Review the configuration. Check the logs and debug."),
    PAGING_ERROR_EXCEPTION(400, "OMRS-HMS-REPOSITORY-400-077 ",
                             "Paging error exception",
                             "Connector is unable to be used",
                             "Review the configuration around paging. Check the logs and debug."),
    FUNCTION_NOT_SUPPORTED_ERROR_EXCEPTION(400, "OMRS-HMS-REPOSITORY-400-079 ",
                                         "Function not supported error exception",
                                         "Connector is unable to be used",
                                         "Review the configuration. Check the logs and debug."),
    USER_NOT_AUTHORIZED_EXCEPTION(400, "OMRS-HMS-REPOSITORY-400-080 ",
                                         "user not authorized error exception",
                                         "Connector is unable to be used",
                                         "Review the configuration. Check the logs and debug."),
    HOME_ENTITY_ERROR_EXCEPTION(400, "OMRS-HMS-REPOSITORY-400-081 ",
                                "Reference copy requests have been issued on a home entity  error exception",
                                "Connector is unable to be used",
                                "Logic error. Check the logs and debug."),
    ENTITY_CONFLICT_ERROR_EXCEPTION(400, "OMRS-HMS-REPOSITORY-400-082 ",
                                    "Entity cannot be added as it conflicts with an entity that has already been stored",
                                    "Connector is unable to be used",
                                    "Logic error. Check the logs and debug."),

    INVALID_ENTITY_ERROR_EXCEPTION(400, "OMRS-HMS-REPOSITORY-400-083 ",
                                    "Entity cannot be added as it is invalid",
                                    "Connector is unable to be used",
                                    "Logic error. Check the logs and debug."),

    HOME_RELATIONSHIP_ERROR_EXCEPTION(400, "OMRS-HMS-REPOSITORY-400-087 ",
                                "Reference copy requests have been issued on a home relationship error exception",
                                "Connector is unable to be used",
                                "Logic error. Check the logs and debug."),
    RELATIONSHIP_CONFLICT_ERROR_EXCEPTION(400, "OMRS-HMS-REPOSITORY-400-088 ",
                                    "Relationship cannot be added as it conflicts with an relationship that has already been stored",
                                    "Connector is unable to be used",
                                    "Logic error. Check the logs and debug."),

    INVALID_RELATIONSHIP_ERROR_EXCEPTION(400, "OMRS-HMS-REPOSITORY-400-089 ",
                                   "Relationship cannot be added as it is invalid",
                                   "Connector is unable to be used",
                                   "Logic error. Check the logs and debug."),

    TYPE_ERROR(400, "OMRS-HMS-REPOSITORY-400-090 ",
                           "A Egeria call was made with an invalid type, the error was {0} ",
                           "Connector is unable to be used",
                           "Raise a Git issue to have this investigated."),

    EVENT_MAPPER_IMPROPERLY_INITIALIZED(400, "OMRS-HMS-REPOSITORY-400-091 ",
                                        "The event mapper has been improperly initialized for repository {0}",
                                        "The system will be unable to process any events",
                                        "Check the system logs and diagnose or report the problem."),

    EMBEDDED_CONNECTOR_NOT_SUPPLIED(400, "OMRS-HMS-REPOSITORY-400-092 ",
                                    "The repository connector expected to have an embedded OMRS connector, but none was configured",
                                    "The system will shutdown the server",
                                    "Amend the configuration to supply an embedded OMRS connector."),

    EMBEDDED_CONNECTOR_WRONG_TYPE(400, "OMRS-HMS-REPOSITORY-400-093 ",
                                    "The embedded connector supp;ied as not an OMRS connector,",
                                    "The system will shutdown the server",
                                    "Amend the configuration to supply an embedded OMRS connector rather than one that is not an OMRS Connector."),
    MULTIPLE_EMBEDDED_CONNECTORS_SUPPLIED(400, "OMRS-HMS-REPOSITORY-400-094 ",
                                    "The repository connector expected to have 1 embedded OMRS connector, but multiple were configured",
                                    "The system will shutdown the server",
                                    "Amend the configuration to supply only 1 embedded OMRS connector."),

    ENCODING_EXCEPTION(400, "OMRS-HMS-REPOSITORY-400-095 ",
                                          "The event mapper failed to encode '{0}' with value '{1}' to create a guid",
                                          "The system will shutdown the server",
                                          "Debug the cause of the encoding error."),
    EVENT_MAPPER_CANNOT_GET_TYPES(400, "OMRS-HMS-REPOSITORY-400-096 ",
                                  "The event mapper failed to obtain the types, so cannot proceed ",
                                  "The system will shutdown the server",
                                  "ensure you are using a repository that supports the required types."),


//    TYPEDEF_NOT_SUPPORTED(404, "OMRS-HMS-REPOSITORY-404-001 ",
//            "On Server {0} for request {1}, the typedef \"{3}\" is not supported by repository \"{1}\"",
//            "The system is currently unable to support the requested the typedef.",
//            "Request support through Egeria GitHub issue."),
    ENTITY_NOT_KNOWN(404, "OMRS-HMS-REPOSITORY-404-002 ",
            "On Server {0} for request {1}, the entity identified with guid {0} is not known to the open metadata repository {2}",
            "The system is unable to retrieve the properties for the requested entity because the supplied guid is not recognized.",
            "The guid is supplied by the caller to the server.  It may have a logic problem that has corrupted the guid, or the entity has been deleted since the guid was retrieved."),

    RELATIONSHIP_NOT_KNOWN(404, "OMRS-HMS-REPOSITORY-404-003 ",
            "On Server {0} for request {1}, the relationship identified with guid {2} is not known to the open metadata repository {2}",
            "The system is unable to retrieve the properties for the requested relationship because the supplied guid is not recognized.",
            "The guid is supplied by the caller to the OMRS.  It may have a logic problem that has corrupted the guid, or the relationship has been deleted since the guid was retrieved."),
    TYPEDEF_NAME_NOT_KNOWN(404, "OMRS-HMS-REPOSITORY-404-004",
                           "On Server {0} for request {1}, the TypeDef unique name {2} passed is not known to this repository connector",
                           "The system is unable to retrieve the properties for the requested TypeDef because the supplied identifiers are not recognized.",
                           "The identifier is supplied by the caller.  It may have a logic problem that has corrupted the identifier, or the TypeDef has been deleted since the identifier was retrieved."),
    ENTITY_PROXY_ONLY(404, "OMRS-HMS-REPOSITORY-404-005",
                      "On server {0} for request {1}, a specific entity instance for guid {2}, but this but only a proxy version of the entity is in the metadata collection",
                      "The system is unable to return the entity as it is only a proxy.",
                      "The guid identifier is supplied by the caller. Amend the caller to supply a guid assoicated with an Entity rather than a proxy."),


    ;

    @SuppressWarnings("ImmutableEnumChecker")
    final private ExceptionMessageDefinition messageDefinition;

    /**
     * The constructor for FileOMRSErrorCode expects to be passed one of the enumeration rows defined in
     * FileOMRSErrorCode above.   For example:
     *
     *     FileOMRSErrorCode   errorCode = FileOMRSErrorCode.NULL_INSTANCE;
     *
     * This will expand out to the 5 parameters shown below.
     *
     * @param newHTTPErrorCode - error code to use over REST calls
     * @param newErrorMessageId - unique Id for the message
     * @param newErrorMessage - text for the message
     * @param newSystemAction - description of the action taken by the system when the error condition happened
     * @param newUserAction - instructions for resolving the error
     */
    DbPollingOMRSErrorCode(int newHTTPErrorCode, String newErrorMessageId, String newErrorMessage, String newSystemAction, String newUserAction) {
        this.messageDefinition = new ExceptionMessageDefinition(newHTTPErrorCode,
                newErrorMessageId,
                newErrorMessage,
                newSystemAction,
                newUserAction);
    }

    /**
     * Retrieve a message definition object for an exception.  This method is used when there are no message inserts.
     *
     * @return message definition object.
     */
    @Override
    public ExceptionMessageDefinition getMessageDefinition() {
        return messageDefinition;
    }


    /**
     * Retrieve a message definition object for an exception.  This method is used when there are values to be inserted into the message.
     *
     * @param params array of parameters (all strings).  They are inserted into the message according to the numbering in the message text.
     * @return message definition object.
     */
    @Override
    public ExceptionMessageDefinition getMessageDefinition(String... params) {
        messageDefinition.setMessageParameters(params);
        return messageDefinition;
    }

    /**
     * toString() JSON-style
     *
     * @return string description
     */
    @Override
    public String toString() {
        return "FileOMRSErrorCode{" +
                "messageDefinition=" + messageDefinition +
                '}';
    }

}
