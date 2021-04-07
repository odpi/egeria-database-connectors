package org.odpi.openmetadata.adapters.connectors.integration.postgres.ffdc;

import org.odpi.openmetadata.commonservices.ffdc.exceptions.OMAGCheckedExceptionBase;
import org.odpi.openmetadata.frameworks.auditlog.messagesets.AuditLogMessageDefinition;
import org.odpi.openmetadata.frameworks.auditlog.messagesets.ExceptionMessageDefinition;

public class AlreadyHanledException extends OMAGCheckedExceptionBase
{

    public AlreadyHanledException(ExceptionMessageDefinition messageDefinition, String name, String methodName, Exception error)
    {
        super(messageDefinition, name, methodName, error);
    }
}
