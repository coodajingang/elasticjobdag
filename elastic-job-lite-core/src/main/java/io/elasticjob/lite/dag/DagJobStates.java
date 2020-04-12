package io.elasticjob.lite.dag;

import org.apache.commons.lang3.StringUtils;

public enum DagJobStates {
    NONE("none"),
    INIT("init"),
    READY("ready"),
    RUNNING("running"),
    PAUSE("pause"),
    FAIL("fail"),
    SUCCESS("success"),
    SKIP("skip"),
    ;

    private String value;
    DagJobStates(String value) {
        this.value = value;
    }

    public static DagJobStates of(String value) {
        for (DagJobStates states : DagJobStates.values()) {
            if (StringUtils.equals(value, states.getValue())) {
                return states;
            }
        }
        return DagJobStates.NONE;
    }
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
