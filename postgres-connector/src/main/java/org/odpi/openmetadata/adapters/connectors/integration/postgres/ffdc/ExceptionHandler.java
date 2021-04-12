package org.odpi.openmetadata.adapters.connectors.integration.postgres.ffdc;

import org.odpi.openmetadata.frameworks.auditlog.AuditLog;
import org.odpi.openmetadata.frameworks.auditlog.messagesets.AuditLogMessageDefinition;
import org.odpi.openmetadata.frameworks.auditlog.messagesets.ExceptionMessageDefinition;

/*
utility class that provides exception logging
 */
public class ExceptionHandler
{
    /**
     @param logger       the audit logger
     @param className    the name of the class that caught the exception
     @param methodName   the name of the method that caught the exception
     @param error        the exception to be logged
     @param auditCodeMsg the audit message
     @param errorCodeMsg the error message
     @throws AlreadyHandledException an exception letting caller methods to know
     **/
    public static void handleException(AuditLog logger, String className, String methodName, Exception error, AuditLogMessageDefinition auditCodeMsg, ExceptionMessageDefinition errorCodeMsg )
            throws AlreadyHandledException
    {
        if( logger != null )
        {
            logger.logException(methodName,
                    auditCodeMsg,
                    error);
        }

        throw new AlreadyHandledException(errorCodeMsg, className, methodName,error);
    }
}
