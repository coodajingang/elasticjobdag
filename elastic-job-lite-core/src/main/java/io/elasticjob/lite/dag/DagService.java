package io.elasticjob.lite.dag;

import io.elasticjob.lite.config.JobCoreConfiguration;
import io.elasticjob.lite.config.JobTypeConfiguration;
import io.elasticjob.lite.reg.base.CoordinatorRegistryCenter;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

/**
 * job dag service
 */
public class DagService {

    private final DagNodeStorage dagNodeStorage;
    private JobDagConfig jobDagConfig;
    private final String jobName;

    public DagService(CoordinatorRegistryCenter regCenter, String jobName, JobDagConfig jobDagConfig) {
        this.jobDagConfig = jobDagConfig;
        this.jobName = jobName;
        dagNodeStorage = new DagNodeStorage(regCenter, jobDagConfig.getDagGroup(), jobName);
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

}
