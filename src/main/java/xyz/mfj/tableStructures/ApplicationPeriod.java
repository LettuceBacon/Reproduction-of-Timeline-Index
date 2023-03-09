package xyz.mfj.tableStructures;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Triple;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

/**
 * 有效时间
 */
public class ApplicationPeriod {
    
    private List<String> applicationPeriodNames;
    private List<String> applicationPeriodStartNames;
    private List<String> applicationPeriodEndNames;

    public ApplicationPeriod(List<Triple<String, String, String>> applicationPeriodList)
    {
        int size = applicationPeriodList.size();
        this.applicationPeriodNames = new ArrayList<>(size);
        this.applicationPeriodStartNames = new ArrayList<>(size);
        this.applicationPeriodEndNames = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            applicationPeriodNames.add(applicationPeriodList.get(i).getLeft());
            applicationPeriodStartNames.add(applicationPeriodList.get(i).getMiddle());
            applicationPeriodEndNames.add(applicationPeriodList.get(i).getRight());
        }
    }

    public boolean containsPeriod(String applicationPeriodName) {
        return applicationPeriodNames.contains(applicationPeriodName);
    }

    public List<String> getApplicationPeriodNames() {
        return this.applicationPeriodNames;
    }

    public String getStartName(String applicationPeriodName) {
        return applicationPeriodStartNames.get(
            applicationPeriodNames.indexOf(applicationPeriodName));
    }

    public String getEndName(String applicationPeriodName) {
        return applicationPeriodEndNames.get(
            applicationPeriodNames.indexOf(applicationPeriodName));
    }
}
