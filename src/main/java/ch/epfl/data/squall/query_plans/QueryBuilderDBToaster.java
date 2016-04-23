package ch.epfl.data.squall.query_plans;

import java.util.LinkedList;
import java.util.List;

import backtype.storm.Config;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.utilities.SquallContext;
import ch.epfl.data.squall.utilities.SystemParameters;


import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponent;
import ch.epfl.data.squall.utilities.StormDBToasterProvider;

public class QueryBuilderDBToaster {

	public static void createDBToasterTopology(List<Component> queryPlan, SquallContext context, Config conf){
		List<DBToasterJoinComponent> dbtComponents = new LinkedList<DBToasterJoinComponent>();
		int planSize = queryPlan.size();
		for(int i = 0; i < planSize; i++){
			Component component = queryPlan.get(i);
			if (component instanceof DBToasterJoinComponent) {
		        dbtComponents.add((DBToasterJoinComponent) component);
		    }
		}
	    if (dbtComponents.size() > 0) StormDBToasterProvider.prepare(context, dbtComponents,
	            SystemParameters.getBoolean(conf, "DIP_DISTRIBUTED"));
	}

}
