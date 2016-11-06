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

package ch.epfl.data.squall.examples.imperative.dbtoaster;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponentFactory;
import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponent;
import ch.epfl.data.squall.components.dbtoaster.DBToasterJoinComponentBuilder;
import ch.epfl.data.squall.components.OperatorComponent;
import ch.epfl.data.squall.expressions.ColumnReference;
import ch.epfl.data.squall.expressions.DateSum;
import ch.epfl.data.squall.expressions.Multiplication;
import ch.epfl.data.squall.expressions.Subtraction;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.expressions.ValueSpecification;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.AggregateSumOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SelectOperator;
import ch.epfl.data.squall.predicates.BetweenPredicate;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.query_plans.QueryPlan;
import ch.epfl.data.squall.query_plans.ThetaQueryPlansParameters;
import ch.epfl.data.squall.types.DateType;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.IntegerType;
import ch.epfl.data.squall.types.LongType;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.StringType;
import ch.epfl.data.squall.types.Type;

public class DBToasterTPCH10Plan extends QueryPlan {
    private static void computeDates() {
	// date2= date1 + 3 months
	String date1Str = "1993-10-01";
	int interval = 3;
	int unit = Calendar.MONTH;

	// setting _date1
	_date1 = _dc.fromString(date1Str);

	// setting _date2
	ValueExpression<Date> date1Ve, date2Ve;
	date1Ve = new ValueSpecification<Date>(_dc, _date1);
	date2Ve = new DateSum(date1Ve, unit, interval);
	_date2 = date2Ve.eval(null);
	// tuple is set to null since we are computing based on constants
    }

    private static Logger LOG = Logger.getLogger(DBToasterTPCH10Plan.class);
    private static final Type<Date> _dc = new DateType();
    private static final NumericType<Double> _doubleConv = new DoubleType();
    private static final StringType _sc = new StringType();
    private static final Type<Long> _lc = new LongType();

    private QueryBuilder _queryBuilder = new QueryBuilder();

    private static final IntegerType _ic = new IntegerType();
    // query variables
    private static Date _date1, _date2;

    private static String MARK = "R";

    public DBToasterTPCH10Plan(String dataPath, String extension, Map conf) {
	computeDates();

	// -------------------------------------------------------------------------------------
	// CUSTKEY, NAME, ADDRESS, NATIONKEY, PHONE, ACCTBAL, COMMENT
	ProjectOperator projectionCustomer = new ProjectOperator(new int[] { 0,
		1, 2, 3, 4, 5, 7 });

	DataSourceComponent relationCustomer = new DataSourceComponent(
		"CUSTOMER", dataPath + "customer" + extension, conf).add(projectionCustomer);
	_queryBuilder.add(relationCustomer);

	// -------------------------------------------------------------------------------------
	SelectOperator selectionOrders = new SelectOperator(
		new BetweenPredicate(new ColumnReference(_dc, 4), true,
			new ValueSpecification(_dc, _date1), false,
			new ValueSpecification(_dc, _date2)));

	
	// ORDERKEY, CUSTKEY
	ProjectOperator projectionOrders = new ProjectOperator(
		new int[] { 0, 1 });

	DataSourceComponent relationOrders = new DataSourceComponent("ORDERS",
		dataPath + "orders" + extension, conf).add(selectionOrders)
		.add(projectionOrders);
	_queryBuilder.add(relationOrders);

	// -------------------------------------------------------------------------------------
	//  NATIONKEY, NAME
	ProjectOperator projectionNation = new ProjectOperator(
		new int[] { 0, 1 });

	DataSourceComponent relationNation = new DataSourceComponent("NATION",
		dataPath + "nation" + extension, conf).add(projectionNation);
	_queryBuilder.add(relationNation);
	// -------------------------------------------------------------------------------------

	SelectOperator selectionLineitem = new SelectOperator(
		new ComparisonPredicate(new ColumnReference(_sc, 8),
			new ValueSpecification(_sc, MARK)));

	// ORDERKEY, EXTENDEDPRICE, DISCOUNT
	ProjectOperator projectionLineitem = new ProjectOperator(new int[] { 0,
		5, 6 });

	DataSourceComponent relationLineitem = new DataSourceComponent(
		"LINEITEM", dataPath + "lineitem" + extension, conf).add(selectionLineitem)
		.add(projectionLineitem);
	_queryBuilder.add(relationLineitem);

	// -------------------------------------------------------------------------------------
    DBToasterJoinComponentBuilder dbToasterCompBuilder = new DBToasterJoinComponentBuilder();
    dbToasterCompBuilder.addRelation(relationCustomer, 
    	new Type[]{_lc, _sc, _sc, _lc, _sc, _sc, _sc},  
    	new String[]{"CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL", "COMMENT"});
    
    dbToasterCompBuilder.addRelation(relationOrders,
    	new Type[]{_lc, _lc}, new String[]{"ORDERKEY", "CUSTKEY"});

    dbToasterCompBuilder.addRelation(relationNation,
    	new Type[]{_lc, _sc}, new String[]{"NATIONKEY", "NAME"});

    dbToasterCompBuilder.addRelation(relationLineitem,
    	new Type[]{_lc, _doubleConv, _doubleConv},
    	new String[]{"ORDERKEY", "EXTENDEDPRICE", "DISCOUNT"});


    dbToasterCompBuilder.setSQL("SELECT CUSTOMER.f0, CUSTOMER.f1, " +
    							"		SUM(LINEITEM.f1 * (1.0 - LINEITEM.f2)), " +
    							"		CUSTOMER.f5, NATION.f1, CUSTOMER.f2, " +
    							"		CUSTOMER.f4, CUSTOMER.f6 " +
    							"FROM CUSTOMER, ORDERS, NATION, LINEITEM " +
    							"WHERE CUSTOMER.f0 = ORDERS.f1 " +
    							"  AND NATION.f0 = CUSTOMER.f3 " +
    							"  AND LINEITEM.f0 = ORDERS.f0 " +
    							"GROUP BY CUSTOMER.f0, " +
    							"		  CUSTOMER.f1, " +
    							"		  CUSTOMER.f5, " +
    							"		  NATION.f1, " +
    							"		  CUSTOMER.f2, " +
    							"	      CUSTOMER.f4, " +
    							"	      CUSTOMER.f6");

    DBToasterJoinComponent dbToasterComponent = dbToasterCompBuilder.build();
    dbToasterComponent.setPrintOut(false);

    _queryBuilder.add(dbToasterComponent);

	AggregateOperator agg = new AggregateSumOperator(new ColumnReference(_doubleConv, 7), conf)
		.setGroupByColumns(Arrays.asList(0, 1, 2, 3, 4, 5, 6));

    OperatorComponent oc = new OperatorComponent(dbToasterComponent, "SUMAGG").add(agg);
    _queryBuilder.add(oc);	
    
    }

    @Override
    public QueryBuilder getQueryPlan() {
	return _queryBuilder;
    }
}
