/*   Copyright (C) 2013-2015 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.security.service.policy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Read authorization rules from a flat file. The file is line-oriented with
 * one rule per line with the form:
 * <pre>
 *     extra_authorization = rule_boolean_expression
 * </pre>
 * where the rule expression is a valid formal visibility expression. Comments
 * may be included by putting them on a line starting them with a
 * <code>#</code> symbol. Comments may not be put on the same line as a rule
 * definition (e.g. "baz = foo &amp; bar # this is a comment"). Blank lines are
 * ignored. A blank boolean expression is considered to always be satisfied,
 * so can be used to always grant an authorization.
 */
public class FlatFileBooleanExpressionAuthorizationRuleReader {

    private final Reader reader;

    /**
     * Construct a new rule reader from a File
     * @param file file to read rules from
     * @throws FileNotFoundException if the specified rule file could not be found
     */
    public FlatFileBooleanExpressionAuthorizationRuleReader(File file) throws FileNotFoundException {
        this(new FileReader(file));
    }

    /**
     * Construct a new rule reader from a Reader
     * @param reader reader to read rules from
     */
    public FlatFileBooleanExpressionAuthorizationRuleReader(Reader reader) {
        this.reader = reader;
    }

    /**
     * Read a set of boolean expression rules from a text file
     * @return a set of rules
     * @throws IOException if the file could not be read
     */
    public BooleanExpressionAuthorizationRuleSet readRules() throws IOException {
        PropertiesFileLoader loader = new PropertiesFileLoader();
        loader.load(reader);

        BooleanExpressionAuthorizationRuleSet rules = new BooleanExpressionAuthorizationRuleSet();
        for (Map.Entry<String, String> rule : loader.orderedEntrySet()) {
            rules.addRule(rule.getKey(), rule.getValue());
        }

        return rules;
    }

    /*
     * This is incredibly ghetto, but I need the lines to be read in order.
     * Taken from: http://stackoverflow.com/a/2828051.
     */
    private static class PropertiesFileLoader extends Properties {
        private LinkedHashMap<String, String> entries = new LinkedHashMap<>();

        @Override
        public Object put(Object key, Object value) {
            entries.put((String) key, (String) value);

            return value;
        }

        public Set<Map.Entry<String, String>> orderedEntrySet() {
            return entries.entrySet();
        }
    }
}
