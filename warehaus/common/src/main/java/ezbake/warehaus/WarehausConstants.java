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

package ezbake.warehaus;

/**
 * Declares constants used by the AccumuloWarehaus.
 */
public class WarehausConstants {

    // Warehaus Configuration constants
    public static final String BATCH_WRITER_MAX_MEMORY_KEY = "warehaus.batch.writer.max.memory";
    public static final String BATCH_WRITER_LATENCY_MS_KEY = "warehaus.batch.writer.latency.ms";
    public static final String BATCH_WRITER_WRITE_THREADS_KEY = "warehaus.batch.writer.threads";
    public static final String BATCH_SCANNER_MAX_MEMORY_KEY = "warehaus.batch.scanner.max.memory";
    public static final String PURGE_VISIBILITY_KEY = "warehaus.purge.system.visibility";
    public static final String WAREHAUS_SPLITS_KEY = "warehaus.splits";
    public static final String DEFAULT_WAREHAUS_SPLITS = "a,b,c,d,e,f,0,1,2,3,4,5,6,7,8,9";
    public static final long DEFAULT_WRITER_MAX_MEMORY = 64000000l;
    public static final long DEFAULT_SCANNER_MAX_MEMORY = 128000000l;
    public static final long DEFAULT_LATENCY = 1000l;
    public static final int DEFAULT_WRITE_THREADS = 4;
    public static final int QUERY_THREADS = 10;
    public static final String PARSED_COUNTS = "records";
    public static final String RAW_COUNTS = "raws";
    public static final String VIEW_COUNTS = "views";
    public static final String DELETE_COUNTS = "deletes";
    public static final String TABLE_NAME = "oneaa_warehaus";
    public static final String PURGE_TABLE_NAME = "oneaa_warehaus_purge";
}
