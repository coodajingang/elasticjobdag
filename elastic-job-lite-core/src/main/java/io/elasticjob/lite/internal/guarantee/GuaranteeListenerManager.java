/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
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
 * limitations under the License.
 * </p>
 */

package io.elasticjob.lite.internal.guarantee;

import io.elasticjob.lite.api.listener.AbstractDistributeOnceElasticJobListener;
import io.elasticjob.lite.api.listener.ElasticJobListener;
import io.elasticjob.lite.dag.DagJobStates;
import io.elasticjob.lite.dag.DagNodeStorage;
import io.elasticjob.lite.internal.listener.AbstractJobListener;
import io.elasticjob.lite.internal.listener.AbstractListenerManager;
import io.elasticjob.lite.reg.base.CoordinatorRegistryCenter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;

import java.util.List;

/**
 * 保证分布式任务全部开始和结束状态监听管理器.
 * 
 * @author zhangliang
 */
@Slf4j
public final class GuaranteeListenerManager extends AbstractListenerManager {
    
    private final GuaranteeNode guaranteeNode;
    
    private final List<ElasticJobListener> elasticJobListeners;

    private final String groupName;
    private final String jobName;
    private final DagNodeStorage dagNodeStorage;
    
    public GuaranteeListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName, final List<ElasticJobListener> elasticJobListeners) {
        super(regCenter, jobName);
        this.guaranteeNode = new GuaranteeNode(jobName);
        this.elasticJobListeners = elasticJobListeners;
        this.groupName = null;
        this.jobName = jobName;
        this.dagNodeStorage = null;
    }

    public GuaranteeListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName, final List<ElasticJobListener> elasticJobListeners, final String groupName) {
        super(regCenter, jobName);
        this.guaranteeNode = new GuaranteeNode(jobName);
        this.elasticJobListeners = elasticJobListeners;
        this.groupName = groupName;
        this.jobName = jobName;
        if (StringUtils.isNotEmpty(this.groupName)) {
            this.dagNodeStorage = new DagNodeStorage(regCenter, groupName, jobName);
        } else {
            this.dagNodeStorage = null;
        }
    }

    @Override
    public void start() {
        addDataListener(new StartedNodeRemovedJobListener());
        addDataListener(new CompletedNodeRemovedJobListener());
    }
    
    class StartedNodeRemovedJobListener extends AbstractJobListener {
        
        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (Type.NODE_REMOVED == eventType && guaranteeNode.isStartedRootNode(path)) {
                for (ElasticJobListener each : elasticJobListeners) {
                    if (each instanceof AbstractDistributeOnceElasticJobListener) {
                        ((AbstractDistributeOnceElasticJobListener) each).notifyWaitingTaskStart();
                    }
                }
            }
        }
    }
    
    class CompletedNodeRemovedJobListener extends AbstractJobListener {
        
        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (Type.NODE_REMOVED == eventType && guaranteeNode.isCompletedRootNode(path)) {
                for (ElasticJobListener each : elasticJobListeners) {
                    if (each instanceof AbstractDistributeOnceElasticJobListener) {
                        ((AbstractDistributeOnceElasticJobListener) each).notifyWaitingTaskComplete();
                    }
                }

                // job 完成事件 判断是否dag ，是的化更新 为完成
                if (StringUtils.isNotEmpty(groupName)) {
                    log.debug("Job Completed, is DAG {} job-{}", groupName, jobName);
                    dagNodeStorage.updateDagJobStates(jobName);
                }
            }
        }
    }
}
