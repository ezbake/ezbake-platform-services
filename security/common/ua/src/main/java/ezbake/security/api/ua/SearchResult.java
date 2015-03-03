/*   Copyright (C) 2013-2014 Computer Sciences Corporation
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

package ezbake.security.api.ua;

import java.util.HashSet;
import java.util.Set;

/**
 * User: jhastings
 * Date: 5/6/14
 * Time: 3:57 PM
 */
public class SearchResult {

    private Set<String> data = new HashSet<>();
    private boolean resultOverflow = false;
    private boolean error = false;
    private int statusCode = 0;

    public Set<String> getData() {
        return data;
    }

    public SearchResult setData(Set<String> data) {
        this.data = data;
        return this;
    }

    public boolean isResultOverflow() {
        return resultOverflow;
    }

    public SearchResult setResultOverflow(boolean resultOverflow) {
        this.resultOverflow = resultOverflow;
        return this;
    }

    public boolean isError() {
        return error;
    }

    public SearchResult setError(boolean error) {
        this.error = error;
        return this;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public SearchResult setStatusCode(int statusCode) {
        this.statusCode = statusCode;
        return this;
    }
}
