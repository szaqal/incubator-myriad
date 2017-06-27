/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.myriad.scheduler;

import java.util.Set;
import javax.inject.Inject;
import org.apache.mesos.Protos;
import org.apache.myriad.configuration.NodeManagerConfiguration;
import org.apache.myriad.state.SchedulerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Rebalancer} is responsible for scaling registered YARN clusters as per
 * configured rules and policies.
 */
public class Rebalancer implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Rebalancer.class);

  private final SchedulerState schedulerState;
  private final MyriadOperations myriadOperations;
  private final ServiceProfileManager profileManager;

  @Inject
  public Rebalancer(SchedulerState schedulerState, MyriadOperations myriadOperations, ServiceProfileManager profileManager) {
    this.schedulerState = schedulerState;
    this.myriadOperations = myriadOperations;
    this.profileManager = profileManager;
  }

  @Override
  public void run() {
    final Set<Protos.TaskID> activeIds = schedulerState.getActiveTaskIds(NodeManagerConfiguration.DEFAULT_NM_TASK_PREFIX);
    final Set<Protos.TaskID> pendingIds = schedulerState.getPendingTaskIds(NodeManagerConfiguration.DEFAULT_NM_TASK_PREFIX);
    LOGGER.info("Active {}, Pending {}", activeIds.size(), pendingIds.size());
    if (activeIds.size() < 1 && pendingIds.size() < 1) {
      myriadOperations.flexUpCluster(profileManager.get("small"), 1, null);
    }
    
  }
}
