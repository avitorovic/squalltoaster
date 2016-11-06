/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.examples.imperative.squallui;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.OperatorComponent;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.operators.AggregateCountOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.RedisOperator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.query_plans.ThetaQueryPlansParameters;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;


public class TradionalHypercubeThetaHyracksPlanRedis extends QueryPlan {
    private static Logger LOG = Logger.getLogger(TradionalHypercubeThetaHyracksPlanRedis.class);

    private final QueryBuilder _queryBuilder = new QueryBuilder();

    private static final IntegerType _ic = new IntegerType();
    private static final LongType _lc = new LongType();
    private static final StringType _sc = new StringType();

    public TradionalHypercubeThetaHyracksPlanRedis(String dataPath, String extension, Map conf) {
	final int Theta_JoinType = ThetaQueryPlansParameters
		.getThetaJoinType(conf);
	// -------------------------------------------------------------------------------------
	// start of query plan filling
	final ProjectOperator projectionCustomer = new ProjectOperator(
		new int[] { 0, 6 });
	final List<Integer> hashCustomer = Arrays.asList(0);
	final DataSourceComponent relationCustomer = new DataSourceComponent(
		"CUSTOMER", dataPath + "customer" + extension, conf).add(
		projectionCustomer).setOutputPartKey(hashCustomer);
	_queryBuilder.add(relationCustomer);

	// -------------------------------------------------------------------------------------
	final ProjectOperator projectionOrders = new ProjectOperator(
		new int[] { 1 });
	final List<Integer> hashOrders = Arrays.asList(0);
	final DataSourceComponent relationOrders = new DataSourceComponent(
		"ORDERS", dataPath + "orders" + extension, conf)
		.add(projectionOrders).setOutputPartKey(hashOrders);
	_queryBuilder.add(relationOrders);

	// -------------------------------------------------------------------------------------

	final ColumnReference colCustomer = new ColumnReference(_ic, 0);
	final ColumnReference colOrders = new ColumnReference(_ic, 0);
	final ComparisonPredicate comp = new ComparisonPredicate(
		ComparisonPredicate.EQUAL_OP, colCustomer, colOrders);

	final AggregateCountOperator agg = new AggregateCountOperator(conf)
		.setGroupByColumns(Arrays.asList(1));


	ThetaJoinComponentFactory thetaBuilder = new ThetaJoinComponentFactory();
	// Join keys should have the same name
    thetaBuilder.addRelation(relationCustomer, new Type[]{_lc, _sc}, new String[]{"column1", "column2"});
    thetaBuilder.addRelation(relationOrders, new Type[]{_lc}, new String[]{"column1"});
        

	Component lastJoiner = thetaBuilder
		.createThetaJoinOperator(relationCustomer,
			relationOrders, _queryBuilder).add(agg)
		.setJoinPredicate(comp)
		.setContentSensitiveThetaJoinWrapper(_ic);

	lastJoiner.setPrintOut(false);

	// -------------------------------------------------------------------------------------
    // Redis stuff
    RedisOperator redis = new RedisOperator(conf);
    OperatorComponent pc = new OperatorComponent(lastJoiner, "SENDRESULTSTOREDIS").add(redis);
     _queryBuilder.add(pc);
    }

    @Override
    public QueryBuilder getQueryPlan() {
	return _queryBuilder;
    }

}
