package io.elasticjob.lite.dag;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.elasticjob.lite.config.JobCoreConfiguration;
import io.elasticjob.lite.config.JobTypeConfiguration;
import io.elasticjob.lite.exception.DagJobCycleException;
import io.elasticjob.lite.exception.DagJobStateException;
import io.elasticjob.lite.exception.DagJobSuccessException;
import io.elasticjob.lite.internal.election.LeaderService;
import io.elasticjob.lite.internal.sharding.ShardingNode;
import io.elasticjob.lite.reg.base.CoordinatorRegistryCenter;
import io.elasticjob.lite.util.concurrent.BlockUtils;
import io.elasticjob.lite.util.env.IpUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.lang.management.ManagementFactory;
import java.util.*;

/**
 * job dag service
 */
@Slf4j
public class DagService {

    private final DagNodeStorage dagNodeStorage;
    private JobDagConfig jobDagConfig;
    private final String jobName;
    private final String groupName;
    private final LeaderService leaderService;

    public DagService(CoordinatorRegistryCenter regCenter, String jobName, JobDagConfig jobDagConfig) {
        this.jobDagConfig = jobDagConfig;
        this.jobName = jobName;
        dagNodeStorage = new DagNodeStorage(regCenter, jobDagConfig.getDagGroup(), jobName);
        leaderService = new LeaderService(regCenter, jobName);
        this.groupName = jobDagConfig.getDagGroup();
    }

    /**
     * 判断当前job是否为dag
     * @return
     */
    public boolean isDagJob() {
        return StringUtils.isNotEmpty(jobDagConfig.getDagGroup());
    }

    /**
     * 判断当前job是否为dag root job
     * @return
     */
    public boolean isDagRootJob() {
        return StringUtils.equals(genDependenciesString(), "self");
    }

    /**
     * current batch status; /dag/groupname/status
     * @return
     */
    public DagJobStates getDagStates() {
        return DagJobStates.of(this.dagNodeStorage.currentDagStates());
    }

    public boolean needDagReGraph() {
        return this.dagNodeStorage.needReGraph();
    }

    public void setNeedDagReGraph() {
        this.dagNodeStorage.setNeedReGraph();
    }

    /**
     * always overwrite
     */
    public void regDagConfig() {
        this.dagNodeStorage.persistDagConfig(genDependenciesString());
    }

    /**
     * 注册dag配置信息 , 根据zk中的配置
     * @param config
     */
    public void regDagConfig(JobTypeConfiguration config) {
       Optional<JobCoreConfiguration> optionalJobCoreConfiguration = Optional.ofNullable(config.getCoreConfig());
       if (optionalJobCoreConfiguration.isPresent()) {
           jobDagConfig = optionalJobCoreConfiguration.get().getJobDagConfig();
       }

       if (isDagJob()) {
           // /dag/reg/jobname value dependency skip
            this.dagNodeStorage.persistDagConfig(genDependenciesString());
       }
    }

    private String genDependenciesString() {
        if (jobDagConfig == null || jobDagConfig.getDagDependencies() == null) {
            return "";
        }
        return StringUtils.join(jobDagConfig.getDagDependencies(), ",");
    }

    /**
     * select leader ;
     * change
     */
    public void changeGroupStateAndReGraph() {
        if (!leaderService.isLeaderUntilBlock()) {
            blockUntilCompleted();
            return;
        }
        String batchNo = getBatchNo();
        dagNodeStorage.setDagStateRegraphProcess();
        if (needDagReGraph()) {
            Map<String, Set<String>> allDagNode = dagNodeStorage.getAllDagNode();
            // check cycle
            checkCycle(allDagNode);
            // graph
            dagNodeStorage.reCreateDagRunGraph(batchNo);
            // add runpath and listener; delete needDagRegraph
            dagNodeStorage.genDagGrapRunJob(allDagNode);
        } else {
            dagNodeStorage.updateDagRunGraphBatchNo(batchNo);
            // rest job run states
            dagNodeStorage.resetAllDagRunJobStates();
        }

        // In transaction do 1.delete process 2.change state running
        dagNodeStorage.changeStateToRunning();
    }

    /**
     * check dag has cycle?
     * @param allDagNode
     */
    public void checkCycle(Map<String, Set<String>> allDagNode) {
        HashMap<String, Set<String>> cloneMap = Maps.newHashMap(allDagNode);

        while (removeSelf(cloneMap)) {
        }
        if (!cloneMap.isEmpty()) {
            log.error("Dag {} find cycle {}", jobDagConfig.getDagGroup(), cloneMap.keySet().size());
            printCycleNode(cloneMap);
            throw new DagJobCycleException("Dag job find cycle");
        }
        log.info("Dag {} checkCycle success", jobDagConfig.getDagGroup());
    }

    private void printCycleNode(HashMap<String, Set<String>> cloneMap) {
        cloneMap.forEach((k,v) -> {
            log.error("{} has cycle with {}", k, Joiner.on("|").join(v));
        });
    }

    private boolean removeSelf(HashMap<String, Set<String>> cloneMap) {
        Iterator<Map.Entry<String, Set<String>>> iterator = cloneMap.entrySet().iterator();
        boolean removed = false;
        while(iterator.hasNext()) {
            Map.Entry<String, Set<String>> next = iterator.next();
            Set<String> value = next.getValue();
            value.remove("self");
            if (value.isEmpty()) {
                markKeyAsSelf(cloneMap, next.getKey());
                iterator.remove();
                removed = true;
            }
        }
        return removed;
    }

    private void markKeyAsSelf(HashMap<String, Set<String>> cloneMap, final String key) {
        cloneMap.values().forEach(s -> s.remove(key));
    }

    private String getBatchNo() {
        String date = DateFormatUtils.format(new Date(), "yyMMddHHmmss");
        return this.jobDagConfig.getDagGroup() + IpUtils.getIp() + ManagementFactory.getRuntimeMXBean().getName().split("@")[0] + date;
    }
    private void blockUntilCompleted() {
        while (!leaderService.isLeaderUntilBlock() || getDagStates() != DagJobStates.RUNNING || dagNodeStorage.isDagStateRegraphProcess()) {
            log.debug("DAG '{}' sleep short time until DAG graph completed.", jobDagConfig.getDagGroup());
            BlockUtils.waitingShortTime();
        }
    }

    private void blockUntilJobStatesSet() {
        while (!leaderService.isLeaderUntilBlock() ||  dagNodeStorage.getDagJobRunStates(null) != DagJobStates.RUNNING ) {
            log.debug("DAG '{}' sleep short time until job {} set Runing.", jobDagConfig.getDagGroup(), jobName);
            BlockUtils.waitingShortTime();
        }
    }

    public void checkJobDependenciesState() {
        if (isDagRootJob()) {
            log.debug("DAG {} job {} is root,no dep.", jobDagConfig.getDagGroup(), jobName);
            return;
        }

        // 要求dep skip 或 success
        String[] deps = dagNodeStorage.getJobDenpendencies();
        for (String dep : deps) {
            if (StringUtils.equals(dep, "self")) {
                continue;
            }
            DagJobStates jobRunStates = dagNodeStorage.getDagJobRunStates(dep);
            if (jobRunStates != DagJobStates.SUCCESS && jobRunStates != DagJobStates.SKIP) {
                log.info("DAG- {} job- {} Dependens job- {} Not ready!", jobDagConfig.getDagGroup(), jobName, dep);
                throw new DagJobStateException("Dag Job DEP not Ready");
            }
        }
    }

    public void checkAndChangeJobStateWhenRun() {
        DagJobStates jobRunStates = dagNodeStorage.getDagJobRunStates(null);
        if (jobRunStates == DagJobStates.SUCCESS) {
            log.info("DAG- {} job- {} already Success!", jobDagConfig.getDagGroup(), jobName);
            throw new DagJobSuccessException("DAG job already Success!" + jobDagConfig.getDagGroup() + jobName );
        }

        if (jobRunStates == DagJobStates.RUNNING) {
            return;
        }
        log.debug("DAG- {} job- {} current states {}", jobDagConfig.getDagGroup(), jobName, jobRunStates);
        if (!leaderService.isLeaderUntilBlock()) {
            blockUntilJobStatesSet();
            return;
        }
        dagNodeStorage.updateDagJobStates(jobName, DagJobStates.RUNNING);
    }


    public DagNodeStorage getDagNodeStorage() {
        return dagNodeStorage;
    }

    public JobDagConfig getJobDagConfig() {
        return jobDagConfig;
    }

    public String getJobName() {
        return jobName;
    }

    public String getGroupName() {
        return groupName;
    }
}
