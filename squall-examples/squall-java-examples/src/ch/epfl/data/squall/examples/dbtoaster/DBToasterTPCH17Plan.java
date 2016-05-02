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

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.OperatorComponent;
import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponentBuilder;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;

import java.util.Map;

/*
 SELECT SUM(L_EXTENDEDPRICE)/7.0 AS AVG_YEARLY FROM LINEITEM, PART
WHERE P_PARTKEY = L_PARTKEY AND P_BRAND = 'Brand#44' AND P_CONTAINER = 'WRAP PKG'
AND L_QUANTITY < (SELECT 0.2*AVG(L_QUANTITY) FROM LINEITEM WHERE L_PARTKEY = P_PARTKEY)
 */
public class DBToasterTPCH17Plan extends QueryPlan {

    private final QueryBuilder _queryBuilder = new QueryBuilder();
    private static final Type<Long> _long = new LongType();
    private static final Type<Double> _double = new DoubleType();
    private static final Type<String> _string = new StringType();
    private static final String P_BRAND = "Brand#44";
    private static final String P_CONTAINER = "WRAP PKG";


    public DBToasterTPCH17Plan(String dataPath, String extension, Map conf) {

        final ProjectOperator projectionNestedLineitem = new ProjectOperator(
                new int[] { 1, 4 }); // partkey, quantity

        final DataSourceComponent relationNestedLineItem = new DataSourceComponent(
                "NESTED_L", dataPath + "lineitem" + extension)
                .setOutputPartKey(0).add(projectionNestedLineitem);
        _queryBuilder.add(relationNestedLineItem);

        //----------------------------------------------------------------------------

        DBToasterJoinComponentBuilder builder = new DBToasterJoinComponentBuilder();
        builder.addRelation(relationNestedLineItem, _long, _long); // partkey, quantity
        builder.setComponentName("L_AVG");
        builder.setSQL("SELECT NESTED_L.f0, 0.2 * AVG(NESTED_L.f1) FROM NESTED_L GROUP BY NESTED_L.f0");
        Component nestedL_avg = builder.build().setOutputPartKey(0);
        _queryBuilder.add(nestedL_avg);

        //----------------------------------------------------------------------------

        final ProjectOperator projectionOuterLineitem = new ProjectOperator(
                new int[] {1, 4, 5}); // partkey, quantity, extended price

        final DataSourceComponent relationOuterLineItem = new DataSourceComponent(
                "LINEITEM", dataPath + "lineitem" + extension)
                .setOutputPartKey(0).add(projectionOuterLineitem);
        _queryBuilder.add(relationOuterLineItem);

        // ---------------------------------------------------------------------------

        final SelectOperator selectionPBrand = new SelectOperator(
                new ComparisonPredicate(new ColumnReference(_string, 3),
                        new ValueSpecification(_string, P_BRAND)));

        final SelectOperator selectionPContainer = new SelectOperator(
                new ComparisonPredicate(new ColumnReference(_string, 6),
                        new ValueSpecification(_string, P_CONTAINER)));

        final ProjectOperator projectionPart = new ProjectOperator(
                new int[] {0}); // partkey

        final DataSourceComponent relationPart = new DataSourceComponent(
                "PART", dataPath + "part" + extension)
                .setOutputPartKey(0)
                .add(selectionPBrand)
                .add(selectionPContainer)
                .add(projectionPart);
        _queryBuilder.add(relationPart);

        // ------------------------------------------------------------------

        builder = new DBToasterJoinComponentBuilder(conf);

        // addAggregatedRelation by default uses AggregateSumOperator.
        // As the aggregate function of the nested relation is Average, the partitioning Scheme of L_AVG must be Key partiitoning.
        // The changes need to be done in order to use the different part scheme in case of nested average:
        // - Implements AggregateStream in AggregateAvgOperator
        // - Don't use StormDBToasterJoin for the nested component because DBToaster can not provide information on change of count when calculating average. Modify OperatorComponent which use AggregateAvgOperator to output stream of update
        // - Refactor the addAggregatedRelation method to accept different aggregator.
        builder.addAggregatedRelation(nestedL_avg, _long, _double); // partkey, avg_quantity
        builder.addRelation(relationPart, _long);
        builder.addRelation(relationOuterLineItem, _long, _long, _double); // partkey, quantity, extended price

        builder.setSQL("SELECT SUM(LINEITEM.f2)/7.0 AS AVG_YEARLY FROM LINEITEM, PART, L_AVG WHERE " +
                "PART.f0 = LINEITEM.f0 AND " +
                "PART.f0 = L_AVG.f0 AND LINEITEM.f1 < L_AVG.f1");

        Component P_L_L_AVG = builder.build();
        _queryBuilder.add(P_L_L_AVG);

        AggregateOperator agg = new AggregateSumOperator(new ColumnReference(_double, 0), conf);

        OperatorComponent finalComponent = new OperatorComponent(
                P_L_L_AVG, "FINAL_RESULT").add(agg);
        _queryBuilder.add(finalComponent);


    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}
