/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.customization;

import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class TransferCustomizations {
    private Map<String,List<String>> customizations = new HashMap<>();

    public List<String> getCustomization(String key) {
        return customizations.get(key);
    }

    public void addCustomization(String key, Object customization) {
        if(Constants.INCLUSION_AND_EXCLUSION_NAMES.contains(key)) {
            List<String> processedCustomization = processCustomization(customization);
            customizations.put(key, processedCustomization);
        }
    }

    private List<String> processCustomization(Object customization) {
        List<String> processedCustomization = new ArrayList<>();
        if (customization instanceof String) {
            processedCustomization.add((String)customization);
        } else if (customization instanceof List<?>) {
            List<?> intermediary = (List<?>) customization;
            for(Object intermediaryObject : intermediary) {
                if(intermediaryObject instanceof String) {
                    processedCustomization.add((String)intermediaryObject);
                }
            }
        }
        return processedCustomization;
    }
}
