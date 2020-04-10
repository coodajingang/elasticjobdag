package io.elasticjob.lite.dag;

import io.elasticjob.lite.reg.base.CoordinatorRegistryCenter;

public class DagNodeStorage {
    private final CoordinatorRegistryCenter regCenter;
    private final String jobName;
    private String groupName;

    private static final String ROOT = "dag";
    private static final String DAG_REG = "reg";  // /dag/groupname
    private static final String DAG_CONFIG = "config";


    public DagNodeStorage(CoordinatorRegistryCenter regCenter, String groupName, String jobName) {
        this.regCenter = regCenter;
        this.jobName = jobName;
        this.groupName = groupName;
    }

    /**
     * 登记 /dag/group/job  value=dependencies comm split
     * @param value
     */
    public void persistDagConfig(String value) {
        if (regCenter.isExisted(pathOfDagConfigJob())) {
            regCenter.update(pathOfDagConfigJob(), value);
        } else {
            regCenter.persist(pathOfDagConfigJob(), value);
        }
    }


    private String pathOfDagRoot() {
        return String.format("/%s", ROOT);
    }
    private String pathOfDagReg() {
        return String.format("/%s/%s/%s", ROOT, groupName, DAG_REG);
    }
    private String pathOfDagRegJob() {
        return String.format("/%s/%s/%s/%s", ROOT, groupName, DAG_REG, jobName);
    }
    private String pathOfDagConfig() {
        return String.format("/%s/%s/%s", ROOT, groupName, DAG_CONFIG);
    }
    private String pathOfDagConfigJob() {
        return String.format("/%s/%s/%s/%s", ROOT, groupName, DAG_CONFIG, jobName);
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

}
