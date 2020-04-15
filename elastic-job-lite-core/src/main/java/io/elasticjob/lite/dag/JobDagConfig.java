package io.elasticjob.lite.dag;

import lombok.*;

/**
 * job DAG的属性配置
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@ToString
public class JobDagConfig {
    /** DAG group */
    private String dagGroup;
    /** DAG dependencies */
    private String dagDependencies;
    /** DAG retry policy */
    private String retryClass;
    /** 是否可以独立运行 暂不用  */
    private boolean dagRunAlone;

    /** 失败时是否允许跳过 */
    private boolean dagSkipWhenFail;



}
