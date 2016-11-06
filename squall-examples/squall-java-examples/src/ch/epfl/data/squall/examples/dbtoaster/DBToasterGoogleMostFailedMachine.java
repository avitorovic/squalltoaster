/*
 *
 *  * Copyright (c) 2011-2015 EPFL DATA Laboratory
 *  * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *  *
 *  * All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package ch.epfl.data.squall.examples.imperative.dbtoaster;

import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.OperatorComponent;
import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponent;
import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponentBuilder;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.types.DateLongType;
import ch.epfl.data.squall.types.DateType;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class DBToasterGoogleMostFailedMachine extends QueryPlan {
    private static Logger LOG = Logger.getLogger(DBToasterGoogleMostFailedMachine.class);

    private static final Type<String> _sc = new StringType();
    private static final Type<Long> _lc = new LongType();
    private static final Type<Integer> _ic = new IntegerType();

    private final int SCHEDULING_CLASS = 3;
    private final int EVENT_FAILED = 3;
    private final String NULL_MACHINE_IDs = "";

    private final QueryBuilder _queryBuilder = new QueryBuilder();

    public DBToasterGoogleMostFailedMachine(String dataPath, String extension, Map conf) {

        // -------------------------------------------------------------------------------------
        // job ID
        final ProjectOperator projectionJobEvents = new ProjectOperator(
                new int[]{2});

        SelectOperator selectionSenstiveJobs = new SelectOperator(
                new ComparisonPredicate(new ColumnReference(_ic, 5),
                        new ValueSpecification(_ic, SCHEDULING_CLASS)));

        final DataSourceComponent relationJobEvents = new DataSourceComponent(
                "JOB_EVENTS", dataPath + "job_events" + extension, conf)
                //.add(selectionSenstiveJobs)
                .add(projectionJobEvents);
        
        _queryBuilder.add(relationJobEvents);

        // -------------------------------------------------------------------------------------
        // job ID, machine ID
        final ProjectOperator projectionTaskEvents = new ProjectOperator(
                new int[]{2, 4});

        SelectOperator selectionKilledTasks = new SelectOperator(
                new ComparisonPredicate(new ColumnReference(_ic, 5),
                        new ValueSpecification(_ic, EVENT_FAILED)));

        SelectOperator selectionNonNullMachinesTaskEvent = new SelectOperator(
                new ComparisonPredicate(ComparisonPredicate.NONEQUAL_OP,
                    new ColumnReference(_sc, 4), new ValueSpecification(_sc, NULL_MACHINE_IDs)));

        final DataSourceComponent relationTaskEvents = new DataSourceComponent(
                "TASK_EVENTS", dataPath + "task_events" + extension, conf)
                .add(selectionKilledTasks)
                .add(selectionNonNullMachinesTaskEvent)
                .add(projectionTaskEvents);
        
        _queryBuilder.add(relationTaskEvents);

        // -------------------------------------------------------------------------------------
        // machine ID
        final ProjectOperator projectionMachineEvents = new ProjectOperator(
                new int[]{1, 3});

       SelectOperator selectionNonNullMachines = new SelectOperator(
                new ComparisonPredicate(ComparisonPredicate.NONEQUAL_OP,
                    new ColumnReference(_sc, 1), new ValueSpecification(_sc, NULL_MACHINE_IDs)));

        final DataSourceComponent relationMachineEvents = new DataSourceComponent(
                "MACHINE_EVENTS", dataPath + "machine_events" + extension, conf)
                .add(selectionNonNullMachines)
                .add(projectionMachineEvents);
        
        _queryBuilder.add(relationMachineEvents);

        // -----------------------------------------------------------------------------------
        DBToasterJoinComponentBuilder dbToasterCompBuilder = new DBToasterJoinComponentBuilder();
        dbToasterCompBuilder.addRelation(relationJobEvents, new Type[]{_lc}, new String[]{"jobID"});
        dbToasterCompBuilder.addRelation(relationTaskEvents, new Type[]{_lc, _sc}, new String[]{"jobID", "machineID"});
        dbToasterCompBuilder.addRelation(relationMachineEvents, new Type[]{_sc, _sc}, new String[]{"machineID", "platform"});

        dbToasterCompBuilder.setSQL("SELECT MACHINE_EVENTS.f0, COUNT(*) " + 
                "FROM JOB_EVENTS, TASK_EVENTS, MACHINE_EVENTS " +
                "WHERE JOB_EVENTS.f0 = TASK_EVENTS.f0 " + 
                "AND MACHINE_EVENTS.f0 = TASK_EVENTS.f1 " +
                "GROUP BY MACHINE_EVENTS.f0, MACHINE_EVENTS.f1");

        DBToasterJoinComponent dbToasterComponent = dbToasterCompBuilder.build();
        dbToasterComponent.setPrintOut(false);

        _queryBuilder.add(dbToasterComponent);

        AggregateOperator agg = new AggregateSumOperator(new ColumnReference(_lc, 2), conf)
        .setGroupByColumns(Arrays.asList(0, 1));

        OperatorComponent finalComponent = new OperatorComponent(dbToasterComponent, "FINAL_RESULT").add(agg);
        _queryBuilder.add(finalComponent);
    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
