package io.cyphera.flink;

import io.cyphera.Cyphera;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Flink SQL UDF: cyphera_protect
 *
 * Usage in Flink SQL:
 *   CREATE FUNCTION cyphera_protect AS 'io.cyphera.flink.CypheraProtectFunction';
 *   SELECT cyphera_protect('ssn', ssn_field) FROM my_table;
 */
public class CypheraProtectFunction extends ScalarFunction {

    private transient Cyphera client;

    private Cyphera getClient() {
        if (client == null) {
            client = CypheraLoader.getInstance();
        }
        return client;
    }

    public String eval(String policyName, String value) {
        if (value == null) return null;
        try {
            return getClient().protect(value, policyName);
        } catch (Exception e) {
            return "[error: " + e.getMessage() + "]";
        }
    }
}
