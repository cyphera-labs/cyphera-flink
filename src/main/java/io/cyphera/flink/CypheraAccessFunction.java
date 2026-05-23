package io.cyphera.flink;

import io.cyphera.Cyphera;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Flink SQL UDF: cyphera_access
 *
 * Usage in Flink SQL:
 *   CREATE FUNCTION cyphera_access AS 'io.cyphera.flink.CypheraAccessFunction';
 *   SELECT cyphera_access(protected_ssn) FROM my_table;
 *
 * Header-driven — no configuration name needed for the 1-arg form.
 * The 2-arg form (configurationName, protectedValue) is an escape hatch
 * for headerless configurations.
 */
public class CypheraAccessFunction extends ScalarFunction {

    private transient Cyphera client;

    private Cyphera getClient() {
        if (client == null) {
            client = CypheraLoader.getInstance();
        }
        return client;
    }

    public String eval(String protectedValue) {
        if (protectedValue == null) return null;
        try {
            return getClient().access(protectedValue);
        } catch (Exception e) {
            return "[error: " + e.getMessage() + "]";
        }
    }

    public String eval(String configurationName, String protectedValue) {
        if (protectedValue == null) return null;
        try {
            return getClient().access(protectedValue, configurationName);
        } catch (Exception e) {
            return "[error: " + e.getMessage() + "]";
        }
    }
}
