package com.du.test.dag;

import com.du.test.curator.TestBaseCurator;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.elasticjob.lite.dag.DagService;
import io.elasticjob.lite.dag.JobDagConfig;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

public class TestDag extends TestBaseCurator {

    @Test
    public void test4checkCycle() {
        DagService dagService = new DagService(null, "132", new JobDagConfig());
        System.out.println("normal");
        Map<String, Set<String>> stringSetMap = genNormalDagMap();
        System.out.println(stringSetMap.size());
        dagService.checkCycle(stringSetMap);
        System.out.println(stringSetMap.size());

        System.out.println("cycle");
        dagService.checkCycle(genCycleDagMap());
    }

    private Map<String, Set<String>> genNormalDagMap() {
        Map<String, Set<String>> nodeMap = Maps.newHashMap();
        nodeMap.put("A", Sets.newHashSet("self"));
        nodeMap.put("B", Sets.newHashSet("A"));
        nodeMap.put("C", Sets.newHashSet("A"));
        nodeMap.put("D", Sets.newHashSet("B"));
        nodeMap.put("E", Sets.newHashSet("B"));
        nodeMap.put("F", Sets.newHashSet("B", "C"));
        nodeMap.put("G", Sets.newHashSet("C"));
        nodeMap.put("H", Sets.newHashSet("G", "B", "A"));
        nodeMap.put("I", Sets.newHashSet("G"));
        nodeMap.put("Z", Sets.newHashSet("self"));
        nodeMap.put("X", Sets.newHashSet("B","Z", "D","G"));
        return nodeMap;
    }

    private Map<String, Set<String>> genCycleDagMap() {
        Map<String, Set<String>> nodeMap = Maps.newHashMap();
        nodeMap.put("A", Sets.newHashSet("self"));
        nodeMap.put("B", Sets.newHashSet("A", "H"));
        nodeMap.put("C", Sets.newHashSet("A"));
        nodeMap.put("D", Sets.newHashSet("B"));
        nodeMap.put("E", Sets.newHashSet("B"));
        nodeMap.put("F", Sets.newHashSet("B", "C"));
        nodeMap.put("G", Sets.newHashSet("C"));
        nodeMap.put("H", Sets.newHashSet("G", "B", "A"));
        nodeMap.put("I", Sets.newHashSet("G"));
        nodeMap.put("Z", Sets.newHashSet("self"));
        nodeMap.put("X", Sets.newHashSet("B","Z", "D","G"));
        return nodeMap;
    }
}
