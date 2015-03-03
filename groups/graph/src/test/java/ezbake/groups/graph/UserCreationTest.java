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

package ezbake.groups.graph;

import ezbake.groups.graph.exception.*;
import ezbake.groups.graph.frames.vertex.BaseVertex;
import org.junit.Test;

/**
 * User: jhastings
 * Date: 7/28/14
 * Time: 9:31 PM
 */
public class UserCreationTest extends GraphCommonSetup {

    @Test(expected=VertexExistsException.class)
    public void userDuplicationTest() throws VertexExistsException, InvalidVertexTypeException, UserNotFoundException, AccessDeniedException, IndexUnavailableException, InvalidGroupNameException {
        Object user1 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One");
        Object user2 = graph.addUser(BaseVertex.VertexType.USER, "User1", "User One");
    }

}
