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

import ezbake.security.api.ua.User;
import ezbake.security.api.ua.UserAttributeService;
import ezbake.security.api.ua.UserNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ThreadedUserProfileExecutor extends ThreadedServiceExecutor<String, User> {
	private Logger log = LoggerFactory.getLogger(ThreadedUserProfileExecutor.class);
	private UserAttributeService uaService;
	
	/**
	 * 
	 * @param uaService
	 * @param maxThreads
	 */
	public ThreadedUserProfileExecutor(UserAttributeService uaService, int maxThreads) {
		super(maxThreads);
		this.uaService = uaService;
	}

    public UserAttributeService getUaService() {
        return this.uaService;
    }

	@Override
	public Map<String, User> execute(Set<String> dnSet) {

		List<Future<Set<User>>> futureList = new LinkedList<>();
		Map<String, User> profiles = new HashMap<>();
		
		Map<Integer, Set<String>> partition = partition(dnSet);
		Set<Integer> indices = partition.keySet();
		
		for(Integer i : indices) {
			Set<String> s = partition.get(i);
			Callable<Set<User>> c = new UserProfileInvocation(s, uaService);
			Future<Set<User>> future = pool.submit(c);
			futureList.add(future);
		}
		
		try {
			for(Future<Set<User>> f : futureList) {
				Set<User> up = f.get();
				profiles = merge(profiles, up);
			}
		} catch (InterruptedException|ExecutionException e) {
			log.error("Error: {}", e);
		}

		return profiles;
	}
	
	/**
	 * Partitions the set of dns in to disjoint subsets
	 * @param dns
	 * @return
	 */
	protected Map<Integer, Set<String>> partition(Set<String> dns) {
		Map<Integer, Set<String>> partition = new HashMap<>();
		int length = dns.size();
		int ratio = length/maxThreads;
		int partitionCount = ratio < maxThreads ? ratio+1 : ratio;
		
		for(int i = 0; i < partitionCount; i++) {
			partition.put(i, new HashSet<String>());
		}
		
		int index = 0;
		
		for(String dn : dns) {
			Set<String> s = partition.get(index);
			s.add(dn);
			index = (index+1) % partitionCount;
		}
		
		return partition;
	}

	protected Map<String, User> merge(Map<String, User> map, Set<User> users) {
		
		for(User u : users) {
            if (u.getPrincipal() != null) {
                map.put(u.getPrincipal(), u);
            }
		}
		
		return map;
	}

    protected class UserProfileInvocation implements Callable<Set<User>> {

		protected Set<String> dns;
		protected UserAttributeService uaservice;
		
		protected UserProfileInvocation(Set<String> dns, UserAttributeService uaservice) {
			this.dns = dns;
			this.uaservice = uaservice;
		}

        @Override
		public Set<User> call()  {
            Set<User> ups = new HashSet<>();
            for (String s : this.dns) {
                try {
                    User u = uaservice.getUserProfile(s);
                    ups.add(u);
                } catch (UserNotFoundException e) {
                    log.info("UserProfile not returned for {}", s, e);
                }
            }
			return ups;
		}
	}
}
