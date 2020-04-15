package io.elasticjob.lite.dag;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.elasticjob.lite.exception.DagJobSysException;
import io.elasticjob.lite.exception.JobSystemException;
import io.elasticjob.lite.internal.storage.LeaderExecutionCallback;
import io.elasticjob.lite.internal.storage.TransactionExecutionCallback;
import io.elasticjob.lite.reg.base.CoordinatorRegistryCenter;
import io.elasticjob.lite.reg.exception.RegExceptionHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.state.ConnectionStateListener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DagNodeStorage {
    private final CoordinatorRegistryCenter regCenter;
    private final String jobName;
    private String groupName;

    private static final String ROOT = "dag";
    private static final String DAG_ROOT = "/dag/%s";
    private static final String DAG_REG = "/dag/%s/reg";
    private static final String DAG_CONFIG = "/dag/%s/config";
    private static final String DAG_CONFIG_JOB = "/dag/%s/config/%s";
    private static final String DAG_STATES = "/dag/%s/states";
    private static final String DAG_NEED_REGRAPH = "/dag/%s/needregraph";
    private static final String DAG_RUN = "/dag/%s/run";
    private static final String DAG_GRAPH_PROCESS = "/dag/%s/processing";
    private static final String DAG_RUN_JOB_STATES = "/dag/%s/run/%s/states";
    private static final String DAG_RUN_JOB_FAIL = "/dag/%s/run/%s/failitem";
    private static final String DAG_RUN_JOB_FAIL_ITEM = "/dag/%s/run/%s/failitem/%s";
    private static final String DAG_RUN_JOB = "/dag/%s/run/%s";



    public DagNodeStorage(CoordinatorRegistryCenter regCenter, String groupName, String jobName) {
        this.regCenter = regCenter;
        this.jobName = jobName;
        this.groupName = groupName;
    }

    /**
     * 登记 /dag/group/config/job  value=dependencies comm split
     * @param value
     */
    public void persistDagConfig(String value) {
        if (regCenter.isExisted(pathOfDagConfigJob())) {
            regCenter.update(pathOfDagConfigJob(), value);
        } else {
            regCenter.persist(pathOfDagConfigJob(), value);
        }

        if (!regCenter.isExisted(pathOfDagStates())) {
            regCenter.persist(pathOfDagStates(), "");
        }
    }


    private String pathOfDagRoot() {
        return String.format(DAG_ROOT, groupName);
    }
    private String pathOfDagConfig() {
        return String.format(DAG_CONFIG, groupName);
    }
    private String pathOfDagConfigJob() {
        return String.format(DAG_CONFIG_JOB, groupName, jobName);
    }
    private String pathOfDagStates() {
        return String.format(DAG_STATES, groupName);
    }
    private String pathOfDagNeedReGraph() {
        return String.format(DAG_NEED_REGRAPH, groupName);
    }
    private String pathOfDagReGraphProcessing() {
        return String.format(DAG_GRAPH_PROCESS, groupName);
    }
    private String pathOfDagRun() {
        return String.format(DAG_RUN, groupName);
    }
    private String pathOfDagRunJob() {
        return String.format(DAG_RUN_JOB, groupName, jobName);
    }
    private String pathOfDagRunJob(String jobname) {
        return String.format(DAG_RUN_JOB, groupName, jobname);
    }
    private String pathOfDagRunJobFailItem(String jobname, String item) {
        return String.format(DAG_RUN_JOB_FAIL_ITEM, groupName, jobname, item);
    }
    private String pathOfDagRunJobFail(String jobname) {
        return String.format(DAG_RUN_JOB_FAIL, groupName, jobname);
    }
    private String pathOfDagRunJobStates(String jobname) {
        return String.format(DAG_RUN_JOB_STATES, groupName, jobname);
    }
    private String pathOfDagRunJobStates() {
        return String.format(DAG_RUN_JOB_STATES, groupName, jobName);
    }
    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    /**
     * 更新job 运行分片的状态
     * @param jobname
     * @param item
     * @param isSuccess
     */
    public void saveDagRunJobFailItems(String jobname, String item, boolean isSuccess, String msg) {
        String path = pathOfDagRunJobFailItem(jobname, item);
        boolean isExist = regCenter.isExisted(path);
        if (isSuccess) {
            if (isExist) {
                regCenter.remove(path);
            }
        } else {
            if (!isExist) {
                regCenter.persist(path, msg);
            }
        }
    }

    /**
     * 判断当前job 下有无失败的分片
     * @return
     */
    public boolean hasFailItems(String jobName) {
        return regCenter.getNumChildren(pathOfDagRunJobFail(jobName)) > 0;
    }

    /**
     * get /dag/group/status
     * @return
     */
    public String currentDagStates() {
        if (this.regCenter.isExisted(pathOfDagStates())) {
            return this.regCenter.getDirectly(pathOfDagStates());
        }
        return "";
    }

    public void updateDagStates(String value) {
        if (this.regCenter.isExisted(pathOfDagStates())) {
            this.regCenter.update(pathOfDagStates(), value);
        } else {
            this.regCenter.persist(pathOfDagStates(), value);
        }
    }

    public void updateDagJobStates(String jobname, DagJobStates jobStates) {
        this.regCenter.update(pathOfDagRunJobStates(jobname), jobStates.getValue());
    }

    public void updateDagJobStates(String jobName) {
        if (hasFailItems(jobName)) {
            updateDagJobStates(jobName, DagJobStates.SUCCESS);
        } else {
            updateDagJobStates(jobName, DagJobStates.FAIL);
        }
    }

    public void setNeedReGraph() {
        if (!this.regCenter.isExisted(pathOfDagNeedReGraph())) {
            this.regCenter.persist(pathOfDagNeedReGraph(), "");
        }
    }

    public void removeNeedReGraph() {
        this.regCenter.remove(pathOfDagNeedReGraph());
    }

    public boolean needReGraph() {
        return this.regCenter.isExisted(pathOfDagNeedReGraph());
    }

    public void setDagStateRegraphProcess() {
        this.regCenter.persistEphemeral(pathOfDagReGraphProcessing(), "");
    }

    public boolean isDagStateRegraphProcess() {
        return this.regCenter.isExisted(pathOfDagReGraphProcessing());
    }

    public void reCreateDagRunGraph(String value) {
        if (this.regCenter.isExisted(pathOfDagRun())) {
            this.regCenter.remove(pathOfDagRun());
        }
        this.regCenter.persist(pathOfDagRun(), value);
    }
    public void addDagRunGraph(String jobName, String state) {

    }

    public Map<String, Set<String>> getAllDagNode() {
        HashMap<String, Set<String>> map = Maps.newHashMap();
        List<String> childrenKeys = this.regCenter.getChildrenKeys(pathOfDagConfig());
        childrenKeys.parallelStream().forEach(s -> map.put(s, Sets.newHashSet(StringUtils.split(this.regCenter.get(pathOfDagConfig() + "/" + s), ""))));
        return map;
    }

    public void updateDagRunGraphBatchNo(String batchNo) {
        this.regCenter.update(pathOfDagRun(), batchNo);
    }

    /**
     * 添加dag group run job
     * @param allDagNode
     */
    public void genDagGrapRunJob(Map<String, Set<String>> allDagNode) {
        try {
            CuratorTransactionFinal curatorTransactionFinal = getClient().inTransaction().check().forPath(pathOfDagRoot()).and();
            for (Map.Entry<String, Set<String>> entry : allDagNode.entrySet()) {
                String k = entry.getKey();
                Set<String> v = entry.getValue();
                curatorTransactionFinal.create().forPath(pathOfDagRunJob(k), Joiner.on(",").join(v).getBytes()).and();
                curatorTransactionFinal.create().forPath(pathOfDagRunJobStates(k), DagJobStates.NONE.getValue().getBytes()).and();
            }
            curatorTransactionFinal.delete().forPath(pathOfDagNeedReGraph()).and();
            curatorTransactionFinal.commit();
            //CHECKSTYLE:OFF
        } catch (final Exception ex) {
            //CHECKSTYLE:ON
            throw new DagJobSysException(ex);
        }
    }

    public void changeStateToRunning() {
        try {
            CuratorTransactionFinal curatorTransactionFinal = getClient().inTransaction().check().forPath(pathOfDagRoot()).and();
            if (isDagStateRegraphProcess()) {
                curatorTransactionFinal.delete().forPath(pathOfDagReGraphProcessing()).and();
            }
            curatorTransactionFinal.setData().forPath(pathOfDagStates(), DagJobStates.RUNNING.getValue().getBytes()).and();
            curatorTransactionFinal.commit();
            //CHECKSTYLE:OFF
        } catch (final Exception ex) {
            //CHECKSTYLE:ON
            throw new DagJobSysException(ex);
        }
    }
    /**
     * 在事务中执行操作.
     *
     * @param callback 执行操作的回调
     */
    public void executeInTransaction(final TransactionExecutionCallback callback) {
        try {
            CuratorTransactionFinal curatorTransactionFinal = getClient().inTransaction().check().forPath("/").and();
            callback.execute(curatorTransactionFinal);
            curatorTransactionFinal.commit();
            //CHECKSTYLE:OFF
        } catch (final Exception ex) {
            //CHECKSTYLE:ON
            throw new DagJobSysException(ex);
        }
    }

    /**
     * 在主节点执行操作.
     *
     * @param latchNode 分布式锁使用的作业节点名称
     * @param callback 执行操作的回调
     */
    public void executeInLeader(final String latchNode, final LeaderExecutionCallback callback) {

    }

    private void handleException(final Exception ex) {
        if (ex instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        } else {
            throw new JobSystemException(ex);
        }
    }

    /**
     * 注册连接状态监听器.
     *
     * @param listener 连接状态监听器
     */
    public void addConnectionStateListener(final ConnectionStateListener listener) {
        getClient().getConnectionStateListenable().addListener(listener);
    }



    private CuratorFramework getClient() {
        return (CuratorFramework) regCenter.getRawClient();
    }



    public String[] getJobDenpendencies() {
        return StringUtils.split(this.regCenter.get(pathOfDagRunJob()), ",");
    }

    public DagJobStates getDagJobRunStates(String depJob) {
        if (StringUtils.isEmpty(depJob)) {
            depJob = jobName;
        }
        return DagJobStates.of(this.regCenter.get(pathOfDagRunJobStates(depJob)));
    }

    public void resetAllDagRunJobStates() {
        List<String> childrenKeys = this.regCenter.getChildrenKeys(pathOfDagRun());
        try {
            CuratorTransactionFinal curatorTransactionFinal = getClient().inTransaction().check().forPath(pathOfDagRun()).and();
            for (String job : childrenKeys) {
                curatorTransactionFinal.setData().forPath(pathOfDagRunJobStates(job), DagJobStates.NONE.getValue().getBytes()).and();
            }
            curatorTransactionFinal.commit();
            //CHECKSTYLE:OFF
        } catch (final Exception ex) {
            //CHECKSTYLE:ON
            throw new DagJobSysException(ex);
        }
    }


}
