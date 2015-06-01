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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;

import com.google.common.collect.Sets;

import org.junit.Test;

public class FlatFileBooleanExpressionAuthorizationRuleReaderTest {

    @Test
    public void testCommentsAndBlankLines() throws IOException {
        String rulesString = "# This is a comment\nB = A\n\n\nC = A\n";

        FlatFileBooleanExpressionAuthorizationRuleReader reader =
                new FlatFileBooleanExpressionAuthorizationRuleReader(new StringReader(rulesString));
        BooleanExpressionAuthorizationRuleSet rulesSet = reader.readRules();

        assertEquals(Sets.newHashSet("A", "B", "C"), rulesSet.grantAuthorizations(Sets.newHashSet("A")));
    }

    @Test
    public void testOrderedRead() throws IOException {
        String rulesString = "B = A\nC = B\n";

        FlatFileBooleanExpressionAuthorizationRuleReader reader =
                new FlatFileBooleanExpressionAuthorizationRuleReader(new StringReader(rulesString));
        BooleanExpressionAuthorizationRuleSet rulesSet = reader.readRules();

        assertEquals(Sets.newHashSet("A", "B", "C"), rulesSet.grantAuthorizations(Sets.newHashSet("A")));
    }

    @Test
    public void testBlankExpression() throws IOException {
        String rulesString = "B =\n";

        FlatFileBooleanExpressionAuthorizationRuleReader reader =
                new FlatFileBooleanExpressionAuthorizationRuleReader(new StringReader(rulesString));
        BooleanExpressionAuthorizationRuleSet rulesSet = reader.readRules();

        assertEquals(Sets.newHashSet("A", "B"), rulesSet.grantAuthorizations(Sets.newHashSet("A")));
    }

    @Test
    public void testReadFile() throws IOException {
        String rulesString = "C=A&B\nD=B\nE=C\nF=X\n";

        File tempFile = File.createTempFile("test", ".txt");
        tempFile.deleteOnExit();

        FileWriter writer = new FileWriter(tempFile);
        writer.write(rulesString);
        writer.close();

        FlatFileBooleanExpressionAuthorizationRuleReader reader =
                new FlatFileBooleanExpressionAuthorizationRuleReader(tempFile);
        BooleanExpressionAuthorizationRuleSet ruleSet = reader.readRules();

        assertEquals(Sets.newHashSet("A", "B", "C", "D", "E"), ruleSet.grantAuthorizations(Sets.newHashSet("A", "B")));
        assertTrue(tempFile.delete());
    }
}
