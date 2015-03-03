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

package ezbake.profile.service.exe;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author gdrocella
 * @date 04/25/14
 * @Time 3:28PM
 */
public abstract class BaseServiceExecutor<K, V> {
	protected ExecutorService pool;
	protected int maxThreads;
	
	public BaseServiceExecutor(int maxThreads) {
		this.maxThreads = maxThreads;
		this.pool = Executors.newFixedThreadPool(this.maxThreads);
	}
	
	public int getMaxThreads() {
		return this.maxThreads;
	}
	
	public Future<? extends Object> submit(Callable<? extends Object> c) {
		return this.pool.submit(c);
	}
	
	public void shutdown() {
		this.pool.shutdown();
	}
	
	public abstract Map<K, V> execute(Set<K> execSet);
}
