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

package ezbake.services.search.utils;

public abstract class SSRUtils {

    public static final String SSR_DATE_FIELD = "_ssr_date";
    public static final String SSR_COORDINATE_FIELD = "_ssr_coordinates";
    public static final String SSR_PROVINCE_FIELD = "_ssr_province";
    public static final String SSR_COUNTRY_FIELD = "_ssr_country";
    public static final String SSR_TYPE_FIELD = "_ssr_type";
    public static final String SSR_METADATA_FIELD = "_ssr_metadata";
    public static final String SSR_FIELD = "_ssr";
    public static final String SSR_TIME_OF_INGEST = "_ssr_ingest";

    public static final String ELASTIC_LONGITUDE_DEFAULT = "lon";
    public static final String ELASTIC_LATITUDE_DEFAULT = "lat";
    
    public static final String PURGE_TYPE_FIELD = "purge:type";
    public static final String PURGE_ID_FIELD = "purge:id";
    public static final String PURGE_STATE_FIELD = "purge:state";
    public static final String[] DATE_FORMATS = new String[] {
            "yyyyMMdd’T' HHmmss.SSSZ",
            "ddHHmm'Z' MMM yy"
    };


    public static final String PERCOLATOR_MAIN_INBOX_TYPE_FIELD = "maininbox";
    public static final String PERCOLATOR_MAIN_INBOX_IDS = "percolatorIds";
    public static final String PERCOLAOTR_INDIVIDUAL_INBOX_LAST_FLUSHED = "lastflushed";
    public static final String PERCOLATOR_INDIVIDUAL_INBOX_TYPE_FIELD = "individualinbox";
    public static final String PERCOLATOR_INDIVIDUAL_INBOX_NAME = "name";
    public static final String PERCOLATOR_INDIVIDUAL_INBOX_SEARCH_TEXT = "searchtext";
    public static final String PERCOLATOR_INDIVIDUAL_INBOX_DOC_HITS = "dochits";
    public static final String PERCOLATOR_INDIVIDUAL_INBOX_DOC_ID = "docId";
    public static final String PERCOLATOR_INDIVIDUAL_INBOX_DOC_TITLE = "docTitle";
    public static final String PERCOLATOR_INDIVIDUAL_INBOX_DOC_RESULTDATE = "docResultDate";
    public static final String PERCOLATOR_INDIVIDUAL_INBOX_DOC_TIMEOFINGEST = "docTimeOfIngest";

    public static final String SSR_DEFAULT_TYPE_NAME = "ssr_default";
}
