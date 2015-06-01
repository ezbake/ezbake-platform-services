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

package ezbake.groups.cli.commands;

import com.google.inject.Guice;
import ezbake.configuration.*;
import ezbake.groups.common.GroupNameHelper;
import ezbake.groups.graph.EzGroupsGraphImpl;
import ezbake.groups.graph.EzGroupsGraphModule;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 9/2/14
 * Time: 10:51 AM
 */
public abstract class EzGroupsCommand {
    private static final Logger logger = LoggerFactory.getLogger(EzGroupsCommand.class);
    protected static final GroupNameHelper nameHelper = new GroupNameHelper();
    protected static final int MAX_TRIES = 4;

    @Option(name="-h", aliases="--help", help=true)
    public boolean help = false;

    protected Properties properties;

    public EzGroupsCommand() { }

    public EzGroupsCommand(Properties properties) {
        this.properties = properties;
    }

    public abstract void runCommand() throws EzConfigurationLoaderException;

    public void setConfigurationProperties(Properties configurationProperties) {
        this.properties = configurationProperties;
    }

    protected EzGroupsGraphImpl getGraph() {
        return Guice.createInjector(new EzGroupsGraphModule(properties)).getInstance(EzGroupsGraphImpl.class);
    }


}
