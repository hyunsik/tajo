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

package org.apache.tajo.engine.codegen;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.org.objectweb.asm.Label;
import org.apache.tajo.org.objectweb.asm.Opcodes;
import org.apache.tajo.org.objectweb.asm.Type;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.util.Stack;

import static org.apache.tajo.common.TajoDataTypes.DataType;
import static org.apache.tajo.engine.codegen.TajoGeneratorAdapter.getDescription;
import static org.apache.tajo.engine.eval.FunctionEval.ParamType;

public class EvalCodeGenerator extends SimpleEvalNodeVisitor<EvalCodeGenContext> {
  public static final EvalCodeGenerator instance;

  static {
    instance = new EvalCodeGenerator();
  }

  public static final byte UNKNOWN = 0;
  public static final byte TRUE = 1;
  public static final byte FALSE = 2;

  /** 0 - UNKNOWN, 1 - TRUE, 2 - FALSE */
  @SuppressWarnings("unused")
  public static final byte [] THREE_VALUES = new byte[]  {UNKNOWN, TRUE, FALSE};
  @SuppressWarnings("unused")
  public static final byte [] NOT_LOGIC =    new byte[] {UNKNOWN, FALSE, TRUE};
  @SuppressWarnings("unused")
  public static final byte [][] AND_LOGIC = new byte [][] {
      //          unknown  true     false
      new byte [] {UNKNOWN, UNKNOWN, FALSE},   // unknown
      new byte [] {UNKNOWN, TRUE,    FALSE},   // true
      new byte [] {FALSE,   FALSE,   FALSE}    // false
  };
  @SuppressWarnings("unused")
  public static final byte [][] OR_LOGIC = new byte [][] {
      //          unknown  true     false
      new byte [] {UNKNOWN, TRUE,    UNKNOWN}, // unknown
      new byte [] {TRUE,    TRUE,    TRUE},    // true
      new byte [] {UNKNOWN, TRUE,    FALSE}    // false
  };

  public static void visit(EvalCodeGenContext context, EvalNode eval) {
    instance.visit(context, eval, new Stack<EvalNode>());
  }

  public EvalNode visitBinaryEval(EvalCodeGenContext context, Stack<EvalNode> stack, BinaryEval binaryEval) {
    if (EvalType.isLogicalOperator(binaryEval.getType())) {
      return visitAndOrEval(context, binaryEval, stack);
    } else if (EvalType.isArithmeticOperator(binaryEval.getType())) {
      return visitArithmeticEval(context, binaryEval, stack);
    } else if (EvalType.isComparisonOperator(binaryEval.getType())) {
      return visitComparisonEval(context, binaryEval, stack);
    } else if (binaryEval.getType() == EvalType.CONCATENATE) {
      return visitStringConcat(context, binaryEval, stack);
    } else if (binaryEval.getType() == EvalType.LIKE || binaryEval.getType() == EvalType.SIMILAR_TO
        || binaryEval.getType() == EvalType.REGEX) {
      return visitStringPatternMatch(context, binaryEval, stack);
    } else if (binaryEval.getType() == EvalType.IN) {
      return visitInPredicate(context, binaryEval, stack);
    } else {
      stack.push(binaryEval);
      visit(context, binaryEval.getLeftExpr(), stack);
      visit(context, binaryEval.getRightExpr(), stack);
      stack.pop();
      return binaryEval;
    }
  }

  public EvalNode visitUnaryEval(EvalCodeGenContext context, Stack<EvalNode> stack, UnaryEval unary) {
    stack.push(unary);
    if (unary.getType() == EvalType.CAST) {
      visitCast(context, stack, (CastEval) unary);

    } else if (unary.getType() == EvalType.NOT) {

      visit(context, unary.getChild(), stack);
      int nullFlagId = context.istore();
      int valueId = context.istore();

      Label ifNull = new Label();
      Label endIf = new Label();

      context.emitNullityCheck(ifNull, nullFlagId);

      context.methodvisitor.visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(EvalCodeGenerator.class),
          "NOT_LOGIC", "[B");
      context.iload(valueId);
      context.methodvisitor.visitInsn(Opcodes.BALOAD);
      context.pushNullFlag(true);
      emitGotoLabel(context, endIf);

      emitLabel(context, ifNull);
      context.pushDummyValue(unary.getValueType());
      context.pushNullFlag(false);

      emitLabel(context, endIf);

    } else if (unary.getType() == EvalType.IS_NULL) {
      return visitIsNull(context, (IsNullEval) unary, stack);


    } else if (unary.getType() == EvalType.SIGNED) {
      visit(context, unary.getChild(), stack);

      Label ifNull = new Label();
      Label endIf = new Label();

      context.emitNullityCheck(ifNull);

      SignedEval signed = (SignedEval) unary;
      switch (signed.getValueType().getType()) {
      case BOOLEAN:
      case CHAR:
      case INT1:
      case INT2:
      case INT4: context.methodvisitor.visitInsn(Opcodes.INEG); break;
      case INT8: context.methodvisitor.visitInsn(Opcodes.LNEG); break;
      case FLOAT4: context.methodvisitor.visitInsn(Opcodes.FNEG); break;
      case FLOAT8: context.methodvisitor.visitInsn(Opcodes.DNEG); break;
      default: throw new InvalidEvalException(unary.getType() + " operation to " + signed.getChild() + " is invalid.");
      }

      context.pushNullFlag(true);
      emitGotoLabel(context, endIf);

      emitLabel(context, ifNull);
      context.pushNullFlag(false);

      emitLabel(context, endIf);

    } else {
      super.visit(context, unary, stack);
    }
    stack.pop();
    return unary;
  }

  public EvalNode visitBetween(EvalCodeGenContext context, BetweenPredicateEval between, Stack<EvalNode> stack) {
    EvalNode predicand = between.getPredicand();
    EvalNode begin = between.getBegin();
    EvalNode end = between.getEnd();

    stack.push(between);

    visit(context, predicand, stack);
    final int PREDICAND_NULLFLAG = context.istore();
    final int PREDICAND = context.store(predicand.getValueType());

    visit(context, begin, stack);
    final int BEGIN_NULLFLAG = context.istore();
    final int BEGIN = context.store(begin.getValueType());

    visit(context, end, stack);
    final int END_NULLFLAG = context.istore();
    final int END = context.store(end.getValueType());

    stack.pop();

    Label ifNullCommon = new Label();
    Label ifNotMatched = new Label();

    Label afterEnd = new Label();


    context.emitNullityCheck(ifNullCommon, PREDICAND_NULLFLAG, BEGIN_NULLFLAG, END_NULLFLAG);

    if (between.isSymmetric()) {
      Label ifFirstMatchFailed = new Label();
      Label ifSecondMatchFailed = new Label();
      Label secondCheck = new Label();
      Label finalDisjunctive = new Label();

      //////////////////////////////////////////////////////////////////////////////////////////
      // second check
      //////////////////////////////////////////////////////////////////////////////////////////

      // predicand <= begin
      context.load(begin.getValueType(), BEGIN);
      context.load(predicand.getValueType(), PREDICAND);
      context.ifCmp(predicand.getValueType(), EvalType.LEQ, ifFirstMatchFailed);

      // end <= predicand
      context.load(end.getValueType(), END);
      context.load(predicand.getValueType(), PREDICAND);
      // inverse the operator GEQ -> LTH
      context.ifCmp(predicand.getValueType(), EvalType.GEQ, ifFirstMatchFailed);

      context.push(true);
      emitGotoLabel(context, secondCheck);

      emitLabel(context, ifFirstMatchFailed);
      context.push(false);

      //////////////////////////////////////////////////////////////////////////////////////////
      // second check
      //////////////////////////////////////////////////////////////////////////////////////////
      emitLabel(context, secondCheck);

      // predicand <= end
      context.load(end.getValueType(), END);
      context.load(predicand.getValueType(), PREDICAND);

      // inverse the operator LEQ -> GTH
      context.ifCmp(predicand.getValueType(), EvalType.LEQ, ifSecondMatchFailed);

      // end <= predicand
      context.load(begin.getValueType(), BEGIN);
      context.load(predicand.getValueType(), PREDICAND);
      // inverse the operator GEQ -> LTH
      context.ifCmp(predicand.getValueType(), EvalType.GEQ, ifSecondMatchFailed);

      context.push(true);
      emitGotoLabel(context, finalDisjunctive);

      emitLabel(context, ifSecondMatchFailed);
      context.push(false);

      emitLabel(context, finalDisjunctive);
      context.methodvisitor.visitInsn(Opcodes.IOR);
      context.methodvisitor.visitJumpInsn(Opcodes.IFEQ, ifNotMatched);
    } else {
      // predicand <= begin
      context.load(begin.getValueType(), BEGIN);
      context.load(predicand.getValueType(), PREDICAND);
      context.ifCmp(predicand.getValueType(), EvalType.LEQ, ifNotMatched);

      // end <= predicand
      context.load(end.getValueType(), END);
      context.load(predicand.getValueType(), PREDICAND);
      context.ifCmp(predicand.getValueType(), EvalType.GEQ, ifNotMatched);
    }

    // IF MATCHED
    context.pushBooleanOfThreeValuedLogic(between.isNot() ? false : true);
    context.pushNullFlag(true);
    emitGotoLabel(context, afterEnd);

    emitLabel(context, ifNotMatched); // IF NOT MATCHED
    context.pushBooleanOfThreeValuedLogic(between.isNot() ? true : false);
    context.pushNullFlag(true);
    emitGotoLabel(context, afterEnd);

    emitLabel(context, ifNullCommon); // IF NULL
    context.pushNullOfThreeValuedLogic();
    context.pushNullFlag(false);

    emitLabel(context, afterEnd);

    return between;
  }

  private void emitGotoLabel(EvalCodeGenContext context, Label label) {
    context.methodvisitor.visitJumpInsn(Opcodes.GOTO, label);
  }

  void emitLabel(EvalCodeGenContext context, Label label) {
    context.methodvisitor.visitLabel(label);
  }

  public EvalNode visitCast(EvalCodeGenContext context, Stack<EvalNode> stack, CastEval cast) {
    DataType  srcType = cast.getOperand().getValueType();
    DataType targetType = cast.getValueType();

    if (srcType.equals(targetType)) {
      visit(context, cast.getChild(), stack);
      return cast;
    }

    visit(context, cast.getChild(), stack);

    Label ifNull = new Label();
    Label afterEnd = new Label();
    context.emitNullityCheck(ifNull);

    context.castInsn(srcType, targetType);
    context.pushNullFlag(true);
    emitGotoLabel(context, afterEnd);

    emitLabel(context, ifNull);
    context.pop(srcType);
    context.pushDummyValue(targetType);
    context.pushNullFlag(false);

    emitLabel(context, afterEnd);
    return cast;
  }

  public EvalNode visitField(EvalCodeGenContext context, Stack<EvalNode> stack, FieldEval field) {

    if (field.getValueType().getType() == TajoDataTypes.Type.NULL_TYPE) {
      context.pushNullOfThreeValuedLogic();
      context.pushNullFlag(false);
    } else {

      Column columnRef = field.getColumnRef();
      int fieldIdx;
      if (columnRef.hasQualifier()) {
        fieldIdx = context.schema.getColumnId(columnRef.getQualifiedName());
      } else {
        fieldIdx = context.schema.getColumnIdByName(columnRef.getSimpleName());
      }

      context.methodvisitor.visitVarInsn(Opcodes.ALOAD, 2);
      context.emitIsNullOfTuple(fieldIdx); // It will push 1 if null, and it will push 0 if not null.

      Label ifNull = new Label();
      Label afterAll = new Label();
      // IFNE means if the first item in stack is not 0.
      context.methodvisitor.visitJumpInsn(Opcodes.IFNE, ifNull);

      context.methodvisitor.visitVarInsn(Opcodes.ALOAD, 2);
      context.emitGetValueOfTuple(columnRef.getDataType(), fieldIdx);
      context.pushNullFlag(true); // not null
      context.methodvisitor.visitJumpInsn(Opcodes.GOTO, afterAll);

      context.methodvisitor.visitLabel(ifNull);
      context.pushDummyValue(field.getValueType());
      context.pushNullFlag(false);
      context.methodvisitor.visitJumpInsn(Opcodes.GOTO, afterAll);

      context.methodvisitor.visitLabel(afterAll);
    }
    return field;
  }

  public EvalNode visitAndOrEval(EvalCodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {

    stack.push(evalNode);
    visit(context, evalNode.getLeftExpr(), stack);
    context.pop();
    int LHS = context.istore();

    visit(context, evalNode.getRightExpr(), stack);
    context.pop();
    int RHS = context.istore();
    stack.pop();

    if (evalNode.getType() == EvalType.AND) {
      context.methodvisitor.visitFieldInsn(Opcodes.GETSTATIC,
          org.apache.tajo.org.objectweb.asm.Type.getInternalName(EvalCodeGenerator.class), "AND_LOGIC", "[[B");
    } else if (evalNode.getType() == EvalType.OR) {
      context.methodvisitor.visitFieldInsn(Opcodes.GETSTATIC,
          org.apache.tajo.org.objectweb.asm.Type.getInternalName(EvalCodeGenerator.class), "OR_LOGIC", "[[B");
    } else {
      throw new CompilationError("visitAndOrEval() cannot generate the code at " + evalNode);
    }
    context.load(evalNode.getLeftExpr().getValueType(), LHS);
    context.methodvisitor.visitInsn(Opcodes.AALOAD);
    context.load(evalNode.getRightExpr().getValueType(), RHS);
    context.methodvisitor.visitInsn(Opcodes.BALOAD);    // get three valued logic number from the AND/OR_LOGIC array
    context.methodvisitor.visitInsn(Opcodes.DUP); // three valued logic number x 2, three valued logic number can be null flag.

    return evalNode;
  }

  public static int store(EvalCodeGenContext context, DataType type, int idx) {
    switch (type.getType()) {
    case NULL_TYPE:
    case BOOLEAN:
    case CHAR:
    case INT1:
    case INT2:
    case INT4:
      context.methodvisitor.visitVarInsn(Opcodes.ISTORE, idx);
      break;
    case INT8: context.methodvisitor.visitVarInsn(Opcodes.LSTORE, idx); break;
    case FLOAT4: context.methodvisitor.visitVarInsn(Opcodes.FSTORE, idx); break;
    case FLOAT8: context.methodvisitor.visitVarInsn(Opcodes.DSTORE, idx); break;
    default: context.methodvisitor.visitVarInsn(Opcodes.ASTORE, idx); break;
    }

    return idx + TajoGeneratorAdapter.getWordSize(type);
  }

  public EvalNode visitArithmeticEval(EvalCodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);
    visit(context, evalNode.getLeftExpr(), stack);          // < left_child, push nullflag
    int LHS_NULLFLAG = context.istore();
    int LHS = context.store(evalNode.getLeftExpr().getValueType());

    visit(context, evalNode.getRightExpr(), stack);         // < left_child, right_child, nullflag
    int RHS_NULLFLAG = context.istore();
    int RHS = context.store(evalNode.getRightExpr().getValueType());
    stack.pop();

    Label ifNull = new Label();
    Label afterEnd = new Label();

    context.emitNullityCheck(ifNull, LHS_NULLFLAG, RHS_NULLFLAG);

    context.load(evalNode.getLeftExpr().getValueType(), LHS);
    context.load(evalNode.getRightExpr().getValueType(), RHS);

    int opCode = TajoGeneratorAdapter.getOpCode(evalNode.getType(), evalNode.getValueType());
    context.methodvisitor.visitInsn(opCode);

    context.pushNullFlag(true);
    emitGotoLabel(context, afterEnd);

    emitLabel(context, ifNull);
    context.pushDummyValue(evalNode.getValueType());
    context.pushNullFlag(false);

    emitLabel(context, afterEnd);

    return evalNode;
  }

  public EvalNode visitComparisonEval(EvalCodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack)
      throws CompilationError {

    DataType lhsType = evalNode.getLeftExpr().getValueType();
    DataType rhsType = evalNode.getRightExpr().getValueType();

    if (lhsType.getType() == TajoDataTypes.Type.NULL_TYPE || rhsType.getType() == TajoDataTypes.Type.NULL_TYPE) {
      context.pushNullOfThreeValuedLogic();
      context.pushNullFlag(false);
    } else {
      stack.push(evalNode);
      visit(context, evalNode.getLeftExpr(), stack);                    // < lhs, l_null
      final int LHS_NULLFLAG = context.istore();
      int LHS = context.store(evalNode.getLeftExpr().getValueType());   // <

      visit(context, evalNode.getRightExpr(), stack);                   // < rhs, r_nullflag
      final int RHS_NULLFLAG = context.istore();
      final int RHS = context.store(evalNode.getRightExpr().getValueType());           // <
      stack.pop();

      Label ifNull = new Label();
      Label ifNotMatched = new Label();
      Label afterEnd = new Label();

      context.emitNullityCheck(ifNull, LHS_NULLFLAG, RHS_NULLFLAG);

      context.load(evalNode.getLeftExpr().getValueType(), LHS);             // < lhs
      context.load(evalNode.getRightExpr().getValueType(), RHS);            // < lhs, rhs

      context.ifCmp(evalNode.getLeftExpr().getValueType(), evalNode.getType(), ifNotMatched);

      context.pushBooleanOfThreeValuedLogic(true);
      context.pushNullFlag(true);
      context.methodvisitor.visitJumpInsn(Opcodes.GOTO, afterEnd);

      context.methodvisitor.visitLabel(ifNotMatched);
      context.pushBooleanOfThreeValuedLogic(false);
      context.pushNullFlag(true);
      context.methodvisitor.visitJumpInsn(Opcodes.GOTO, afterEnd);

      context.methodvisitor.visitLabel(ifNull);
      context.pushNullOfThreeValuedLogic();
      context.pushNullFlag(false);

      context.methodvisitor.visitLabel(afterEnd);
    }

    return evalNode;
  }

  public EvalNode visitStringConcat(EvalCodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack)
      throws CompilationError {

    stack.push(evalNode);

    visit(context, evalNode.getLeftExpr(), stack);                    // < lhs, l_null
    final int LHS_NULLFLAG = context.istore();               // < lhs
    final int LHS = context.store(evalNode.getLeftExpr().getValueType());

    visit(context, evalNode.getRightExpr(), stack);                   // < rhs, r_nullflag
    int RHS_NULLFLAG = context.istore();
    int RHS = context.store(evalNode.getRightExpr().getValueType());           // <
    stack.pop();

    Label ifNull = new Label();
    Label afterEnd = new Label();

    context.emitNullityCheck(ifNull, LHS_NULLFLAG, RHS_NULLFLAG);

    context.load(evalNode.getLeftExpr().getValueType(), LHS);                     // < lhs
    context.load(evalNode.getRightExpr().getValueType(), RHS);            // < lhs, rhs

    context.invokeVirtual(String.class, "concat", String.class, new Class[] {String.class});
    context.pushNullFlag(true);
    context.methodvisitor.visitJumpInsn(Opcodes.GOTO, afterEnd);

    context.methodvisitor.visitLabel(ifNull);
    context.pushDummyValue(evalNode.getValueType());
    context.pushNullFlag(false);

    context.methodvisitor.visitLabel(afterEnd);

    return evalNode;
  }

  public EvalNode visitIsNull(EvalCodeGenContext context, IsNullEval isNullEval, Stack<EvalNode> stack) {

    visit(context, isNullEval.getChild(), stack);

    Label ifNull = new Label();
    Label endIf = new Label();

    context.emitNullityCheck(ifNull);

    context.pop(isNullEval.getChild().getValueType());
    context.pushBooleanOfThreeValuedLogic(isNullEval.isNot() ? true : false);
    context.methodvisitor.visitJumpInsn(Opcodes.GOTO, endIf);

    context.methodvisitor.visitLabel(ifNull);
    context.pop(isNullEval.getChild().getValueType());
    context.pushBooleanOfThreeValuedLogic(isNullEval.isNot() ? false : true);

    emitLabel(context, endIf);
    context.methodvisitor.visitInsn(Opcodes.ICONST_1); // NOT NULL

    return isNullEval;
  }


  @Override
  public EvalNode visitConst(EvalCodeGenContext context, ConstEval constEval, Stack<EvalNode> stack) {
    switch (constEval.getValueType().getType()) {
    case NULL_TYPE:

      if (stack.isEmpty()) {
        context.pushNullOfThreeValuedLogic();
      } else {
        EvalNode parentNode = stack.peek();

        if (parentNode instanceof BinaryEval) {
          BinaryEval parent = (BinaryEval) stack.peek();
          if (parent.getLeftExpr() == constEval) {
            context.pushDummyValue(parent.getRightExpr().getValueType());
          } else {
            context.pushDummyValue(parent.getLeftExpr().getValueType());
          }
        } else if (parentNode instanceof CaseWhenEval) {
          CaseWhenEval caseWhen = (CaseWhenEval) parentNode;
          context.pushDummyValue(caseWhen.getValueType());
        } else {
          throw new CompilationError("Cannot find matched type in the stack: " + constEval);
        }
      }
      break;
    case BOOLEAN:
      context.push(constEval.getValue().asInt4());
      break;

    case INT1:
    case INT2:
    case INT4:
    case DATE:
      context.push(constEval.getValue().asInt4());
      break;
    case INT8:
    case TIMESTAMP:
    case TIME:
      context.push(constEval.getValue().asInt8());
      break;
    case FLOAT4:
      context.push(constEval.getValue().asFloat4());
      break;
    case FLOAT8:
      context.push(constEval.getValue().asFloat8());
      break;
    case CHAR:
    case TEXT:
      context.push(constEval.getValue().asChars());
      break;
    case INTERVAL:
      // load pre-stored variable.
      emitGetField(context, context.owner, context.variables.symbols.get(constEval), IntervalDatum.class);
      break;
    default:
      throw new UnsupportedOperationException(constEval.getValueType().getType().name() +
          " const type is not supported");
    }

    context.pushNullFlag(constEval.getValueType().getType() != TajoDataTypes.Type.NULL_TYPE);
    return constEval;
  }

  public static ParamType [] getParamTypes(EvalNode [] arguments) {
    ParamType[] paramTypes = new ParamType[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      if (arguments[i].getType() == EvalType.CONST) {
        if (arguments[i].getValueType().getType() == TajoDataTypes.Type.NULL_TYPE) {
          paramTypes[i] = ParamType.NULL;
        } else {
          paramTypes[i] = ParamType.CONSTANT;
        }
      } else {
        paramTypes[i] = ParamType.VARIABLE;
      }
    }
    return paramTypes;
  }

  @Override
  public EvalNode visitFuncCall(EvalCodeGenContext context, FunctionEval func, Stack<EvalNode> stack) {
    int paramNum = func.getArgs().length;
    context.push(paramNum);
    context.newArray(Datum.class); // new Datum[paramNum]
    final int DATUM_ARRAY = context.astore();

    stack.push(func);
    EvalNode [] params = func.getArgs();
    for (int paramIdx = 0; paramIdx < func.getArgs().length; paramIdx++) {
      context.aload(DATUM_ARRAY);       // array ref
      context.methodvisitor.visitLdcInsn(paramIdx); // array idx
      visit(context, params[paramIdx], stack);
      context.convertToDatum(params[paramIdx].getValueType(), true);  // value
      context.methodvisitor.visitInsn(Opcodes.AASTORE);
    }
    stack.pop();

    context.methodvisitor.visitTypeInsn(Opcodes.NEW, TajoGeneratorAdapter.getInternalName(VTuple.class));
    context.methodvisitor.visitInsn(Opcodes.DUP);
    context.aload(DATUM_ARRAY);
    context.newInstance(VTuple.class, new Class[]{Datum[].class});  // new VTuple(datum [])
    context.methodvisitor.visitTypeInsn(Opcodes.CHECKCAST, TajoGeneratorAdapter.getInternalName(Tuple.class)); // cast to Tuple
    final int TUPLE = context.astore();

    FunctionDesc desc = func.getFuncDesc();

    String fieldName = context.variables.symbols.get(func);
    String funcDescName = "L" + TajoGeneratorAdapter.getInternalName(desc.getFuncClass()) + ";";

    context.aload(0);
    context.methodvisitor.visitFieldInsn(Opcodes.GETFIELD, context.owner, fieldName, funcDescName);
    context.aload(TUPLE);
    context.invokeVirtual(desc.getFuncClass(), "eval", Datum.class, new Class[] {Tuple.class});

    context.convertToPrimitive(func.getValueType());
    return func;
  }

  public EvalNode visitInPredicate(EvalCodeGenContext context, EvalNode patternEval, Stack<EvalNode> stack) {
    String fieldName = context.variables.symbols.get(patternEval);
    emitGetField(context, context.owner, fieldName, InEval.class);
    if (context.schema != null) {
      emitGetField(context, context.owner, "schema", Schema.class);
    } else {
      context.methodvisitor.visitInsn(Opcodes.ACONST_NULL);
    }
    context.aload(2); // tuple
    context.invokeVirtual(InEval.class, "eval", Datum.class, new Class[]{Schema.class, Tuple.class});
    context.convertToPrimitive(patternEval.getValueType());

    return patternEval;
  }

  protected EvalNode visitStringPatternMatch(EvalCodeGenContext context, EvalNode patternEval, Stack<EvalNode> stack) {
    Class clazz = getStringPatternEvalClass(patternEval.getType());
    String fieldName = context.variables.symbols.get(patternEval);
    emitGetField(context, context.owner, fieldName, clazz);
    if (context.schema != null) {
      emitGetField(context, context.owner, "schema", Schema.class);
    } else {
      context.methodvisitor.visitInsn(Opcodes.ACONST_NULL);
    }
    context.aload(2); // tuple
    context.invokeVirtual(clazz, "eval", Datum.class, new Class[]{Schema.class, Tuple.class});
    context.convertToPrimitive(patternEval.getValueType());

    return patternEval;
  }

  protected static void emitGetField(EvalCodeGenContext context, String owner, String fieldName, Class clazz) {
    context.aload(0);
    context.methodvisitor.visitFieldInsn(Opcodes.GETFIELD, owner, fieldName, getDescription(clazz));
  }

  public static Class getStringPatternEvalClass(EvalType type) {
    if (type == EvalType.LIKE) {
      return LikePredicateEval.class;
    } else if (type == EvalType.SIMILAR_TO) {
      return SimilarToPredicateEval.class;
    } else {
      return RegexPredicateEval.class;
    }
  }

  @SuppressWarnings("unused")
  public static EvalNode createEval(String json) {
    return CoreGsonHelper.fromJson(json, EvalNode.class);
  }

  @SuppressWarnings("unused")
  public static ConstEval createConstEval(String json) {
    return (ConstEval) CoreGsonHelper.fromJson(json, EvalNode.class);
  }

  @SuppressWarnings("unused")
  public static RowConstantEval createRowConstantEval(String json) {
    return (RowConstantEval) CoreGsonHelper.fromJson(json, EvalNode.class);
  }

  @SuppressWarnings("unused")
  public static Schema createSchema(String json) {
    return CoreGsonHelper.fromJson(json, Schema.class);
  }

  @Override
  protected EvalNode visitCaseWhen(EvalCodeGenContext context, CaseWhenEval caseWhen, Stack<EvalNode> stack) {
    CaseWhenEmitter.getInstance().emit(this, context, caseWhen, stack);
    return caseWhen;
  }

  @Override
  protected EvalNode visitIfThen(EvalCodeGenContext context, CaseWhenEval.IfThenEval evalNode, Stack<EvalNode> stack) {
    stack.push(evalNode);
    visit(context, evalNode.getCondition(), stack);
    visit(context, evalNode.getResult(), stack);
    stack.pop();
    return evalNode;
  }

}
