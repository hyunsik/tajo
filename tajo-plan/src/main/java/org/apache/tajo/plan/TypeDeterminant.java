/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.tajo.DataTypeUtil;
import org.apache.tajo.algebra.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.*;
import org.apache.tajo.function.FunctionUtil;
import org.apache.tajo.plan.expr.InvalidEvalException;
import org.apache.tajo.plan.nameresolver.NameResolver;
import org.apache.tajo.plan.nameresolver.NameResolvingMode;
import org.apache.tajo.plan.verifier.SyntaxErrorException;
import org.apache.tajo.plan.visitor.SimpleAlgebraVisitor;
import org.apache.tajo.type.Type;

import java.util.Collection;
import java.util.Stack;
import java.util.stream.Collectors;

import static org.apache.tajo.catalog.CatalogUtil.getWidestType;
import static org.apache.tajo.catalog.TypeConverter.convert;
import static org.apache.tajo.common.TajoDataTypes.DataType;
import static org.apache.tajo.common.TajoDataTypes.Type.*;
import static org.apache.tajo.function.FunctionUtil.buildSimpleFunctionSignature;
import static org.apache.tajo.type.Type.*;

public class TypeDeterminant extends SimpleAlgebraVisitor<LogicalPlanner.PlanContext, Type> {
  private DataType BOOL_TYPE = CatalogUtil.newSimpleDataType(BOOLEAN);
  private CatalogService catalog;

  public TypeDeterminant(CatalogService catalog) {
    this.catalog = catalog;
  }

  public Type determineDataType(LogicalPlanner.PlanContext ctx, Expr expr) throws TajoException {
    return visit(ctx, new Stack<>(), expr);
  }

  @Override
  public Type visitUnaryOperator(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, UnaryOperator expr)
      throws TajoException {
    stack.push(expr);
    Type dataType;
    switch (expr.getType()) {
    case IsNullPredicate:
    case ExistsPredicate:
      dataType = Bool;
      break;
    case Cast:
      dataType = LogicalPlanner.convertDataType(((CastExpr)expr).getTarget());
      break;
    default:
      dataType = visit(ctx, stack, expr.getChild());
    }

    return dataType;
  }

  @Override
  public Type visitBinaryOperator(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, BinaryOperator expr)
      throws TajoException {
    stack.push(expr);
    Type lhsType = visit(ctx, stack, expr.getLeft());
    Type rhsType = visit(ctx, stack, expr.getRight());
    stack.pop();
    return computeBinaryType(expr.getType(), lhsType, rhsType);
  }

  public Type computeBinaryType(OpType type, Type lhsDataType, Type rhsDataType) throws TajoException {
    Preconditions.checkNotNull(type);
    Preconditions.checkNotNull(lhsDataType);
    Preconditions.checkNotNull(rhsDataType);

    if(OpType.isLogicalType(type) || OpType.isComparisonType(type)) {
      return Bool;
    } else if (OpType.isArithmeticType(type)) {
      return DataTypeUtil.determineType(lhsDataType, rhsDataType);
    } else if (type == OpType.Concatenate) {
      return Type.Text;
    } else if (type == OpType.InPredicate) {
      return Bool;
    } else if (type == OpType.LikePredicate || type == OpType.SimilarToPredicate || type == OpType.Regexp) {
      return Bool;
    } else {
      throw new TajoInternalError(type.name() + "is not binary type");
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Type visitBetween(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, BetweenPredicate expr)
      throws TajoException {
    return Bool;
  }

  @Override
  public Type visitCaseWhen(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, CaseWhenPredicate caseWhen)
      throws TajoException {
    DataType lastDataType = null;

    final Collection<Type> types = caseWhen.getWhens().stream().map((cond) -> {
      try {
        return visit(ctx, stack, cond.getResult());
      } catch (TajoException e) {
        throw new TajoRuntimeException(e);
      }
    }).collect(Collectors.toList());

    // Record and Array do not have any implicit type casting. All result types and else type must be same.
    long complexTypeNum = types.stream().filter((t) -> t.kind() == RECORD || t.kind() == ARRAY).count();
    if (complexTypeNum > 0) {
      Type prev = null;
      for (Type t: types) {
        if (prev == null) {
          prev = t;
        } else {
          if (!prev.equals(t)) {
            throw new SQLSyntaxError(
                "The result types of case clauses and the result type of else must be equivalent to one another");
          }
        }
      }
    }

    for (Type t : types) {
      if (lastDataType != null) {
        lastDataType = getWidestType(lastDataType, convert(t).getDataType());
      } else {
        lastDataType = convert(t).getDataType();
      }
    }

    if (caseWhen.hasElseResult()) {
      Type elseResultType = visit(ctx, stack, caseWhen.getElseResult());
      lastDataType = getWidestType(lastDataType, convert(elseResultType).getDataType());
    }

    return TypeConverter.convert(lastDataType);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Expressions
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Type visitColumnReference(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, ColumnReferenceExpr expr)
      throws TajoException {
    Column column = NameResolver.resolve(ctx.plan, ctx.queryBlock, expr, NameResolvingMode.LEGACY, true);
    return column.getType();
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // General Set Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Type visitFunction(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, FunctionExpr expr)
      throws TajoException {
    stack.push(expr); // <--- Push

    // Given parameters
    Expr[] params = expr.getParams();
    if (params == null) {
      params = new Expr[0];
    }

    DataType[] givenArgs = new DataType[params.length];
    DataType[] paramTypes = new DataType[params.length];

    for (int i = 0; i < params.length; i++) {
      givenArgs[i] = convert(visit(ctx, stack, params[i])).getDataType();
      paramTypes[i] = givenArgs[i];
    }

    stack.pop(); // <--- Pop

    if (!catalog.containFunction(expr.getSignature(), paramTypes)) {
      throw new UndefinedFunctionException(FunctionUtil.buildSimpleFunctionSignature(expr.getSignature(), paramTypes));
    }

    FunctionDesc funcDesc = catalog.getFunction(expr.getSignature(), paramTypes);
    return convert(funcDesc.getReturnType());
  }

  @Override
  public Type visitCountRowsFunction(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, CountRowsFunctionExpr expr)
      throws TajoException {
    FunctionDesc countRows = catalog.getFunction("count", CatalogProtos.FunctionType.AGGREGATION,
        new DataType[] {});
    return convert(countRows.getReturnType());
  }

  @Override
  public Type visitGeneralSetFunction(LogicalPlanner.PlanContext ctx, Stack<Expr> stack,
                                          GeneralSetFunctionExpr setFunction) throws TajoException {
    stack.push(setFunction);

    Expr[] params = setFunction.getParams();
    DataType[] givenArgs = new DataType[params.length];
    DataType[] paramTypes = new DataType[params.length];

    CatalogProtos.FunctionType functionType = setFunction.isDistinct() ?
        CatalogProtos.FunctionType.DISTINCT_AGGREGATION : CatalogProtos.FunctionType.AGGREGATION;
    givenArgs[0] = convert(visit(ctx, stack, params[0])).getDataType();
    if (setFunction.getSignature().equalsIgnoreCase("count")) {
      paramTypes[0] = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.ANY);
    } else {
      paramTypes[0] = givenArgs[0];
    }

    stack.pop(); // <-- pop

    if (!catalog.containFunction(setFunction.getSignature(), functionType, paramTypes)) {
      throw new UndefinedFunctionException(buildSimpleFunctionSignature(setFunction.getSignature(), paramTypes));
    }

    FunctionDesc funcDesc = catalog.getFunction(setFunction.getSignature(), functionType, paramTypes);
    return convert(funcDesc.getReturnType());
  }

  @Override
  public Type visitWindowFunction(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, WindowFunctionExpr windowFunc)
      throws TajoException {
    stack.push(windowFunc); // <--- Push

    String funcName = windowFunc.getSignature();
    boolean distinct = windowFunc.isDistinct();
    Expr[] params = windowFunc.getParams();
    DataType[] givenArgs = new DataType[params.length];
    TajoDataTypes.DataType[] paramTypes = new TajoDataTypes.DataType[params.length];
    CatalogProtos.FunctionType functionType;

    // Rewrite parameters if necessary
    if (params.length > 0) {
      givenArgs[0] = convert(visit(ctx, stack, params[0])).getDataType();

      if (windowFunc.getSignature().equalsIgnoreCase("count")) {
        paramTypes[0] = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.ANY);
      } else if (windowFunc.getSignature().equalsIgnoreCase("row_number")) {
        paramTypes[0] = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT8);
      } else {
        paramTypes[0] = givenArgs[0];
      }
      for (int i = 1; i < params.length; i++) {
        givenArgs[i] = convert(visit(ctx, stack, params[i])).getDataType();
        paramTypes[i] = givenArgs[i];
      }
    }
    stack.pop(); // <--- Pop

    // TODO - containFunction and getFunction should support the function type mask which provides ORing multiple types.
    // the below checking against WINDOW_FUNCTIONS is a workaround code for the above problem.
    if (ExprAnnotator.WINDOW_FUNCTIONS.contains(funcName.toLowerCase())) {
      if (distinct) {
        throw new UndefinedFunctionException("row_number() does not support distinct keyword.");
      }
      functionType = CatalogProtos.FunctionType.WINDOW;
    } else {
      functionType = distinct ?
          CatalogProtos.FunctionType.DISTINCT_AGGREGATION : CatalogProtos.FunctionType.AGGREGATION;
    }

    if (!catalog.containFunction(windowFunc.getSignature(), functionType, paramTypes)) {
      throw new UndefinedFunctionException(FunctionUtil.buildSimpleFunctionSignature(funcName, paramTypes));
    }

    FunctionDesc funcDesc = catalog.getFunction(funcName, functionType, paramTypes);

    return convert(funcDesc.getReturnType());
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Literal Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Type visitDataType(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, DataTypeExpr expr)
      throws TajoException {
    return LogicalPlanner.convertDataType(expr);
  }

  @Override
  public Type visitLiteral(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, LiteralValue expr)
      throws TajoException {
    // It must be the same to ExprAnnotator::visitLiteral.

    switch (expr.getValueType()) {
    case Boolean:
      return Bool;
    case String:
      return Text;
    case Unsigned_Integer:
      return Int4;
    case Unsigned_Large_Integer:
      return Int8;
    case Unsigned_Float:
      return Float8;
    default:
      throw new RuntimeException("Unsupported type: " + expr.getValueType());
    }
  }

  @Override
  public Type visitNullLiteral(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, NullLiteral expr)
      throws TajoException {
    return Null;
  }

  @Override
  public Type visitTimestampLiteral(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, TimestampLiteral expr)
      throws TajoException {
    return Timestamp;
  }

  @Override
  public Type visitTimeLiteral(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, TimeLiteral expr)
      throws TajoException {
    return Time;
  }

  @Override
  public Type visitDateLiteral(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, DateLiteral expr)
      throws TajoException {
    return Date;
  }

  @Override
  public Type visitIntervalLiteral(LogicalPlanner.PlanContext ctx, Stack<Expr> stack, IntervalLiteral expr)
      throws TajoException {
    return Interval;
  }
}
