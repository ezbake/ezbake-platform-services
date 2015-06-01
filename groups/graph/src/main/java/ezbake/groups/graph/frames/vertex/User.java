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

package ezbake.groups.graph.frames.vertex;

import com.tinkerpop.frames.Property;

/**
 * User: jhastings
 * Date: 6/25/14
 * Time: 3:40 PM
 */
public interface User extends BaseVertex {
    public static final String PRINCIPAL = "principal";
    public static final String USER_NAME = "userName";

    @Property(PRINCIPAL)
    public void setPrincipal(String principal);
    @Property(PRINCIPAL)
    public String getPrincipal();

    @Property(USER_NAME)
    public void setName(String name);
    @Property(USER_NAME)
    public String getName();





}
