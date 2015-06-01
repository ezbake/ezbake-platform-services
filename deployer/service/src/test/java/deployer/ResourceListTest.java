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

package deployer;

import com.google.common.base.Predicate;
import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;


public class ResourceListTest {
    public static final String testPath = "ezbake.deployer.test.resourcelist";

    @Test
    public void shouldFindAllResources() {
        Reflections reflections = new Reflections(
                new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage(testPath))
                        .setScanners(new ResourcesScanner())
                        .filterInputsBy(new Predicate<String>() {
                            @Override
                            public boolean apply(String input) {
                                return input.startsWith(testPath);
                            }
                        })
        );

        System.out.println(reflections.getResources(new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return true;
            }
        }));
    }
}
