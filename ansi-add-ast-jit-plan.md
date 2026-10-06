# Plan: ANSI Add Support with Project AST JIT

## Goal

Enable `GpuProject` AST JIT to preserve ANSI overflow semantics for integral
`GpuAdd`, so a query like the following can use cuDF AST JIT while still throwing
on overflow when `spark.sql.ansi.enabled=true`:

```sql
SELECT a + a FROM mytable
```

The first milestone should target project expressions only. This is experimental
support and should be documented as covering only resolved `GpuAdd` expressions
whose result type is `IntegerType` or `LongType`. Aggregate pre-step
expressions, `ByteType`/`ShortType`, decimals, intervals, floating point, and
full arithmetic coverage can be separate follow-ups.

## Current State

Project AST JIT is selected in
[`basicPhysicalOperators.scala`](sql-plugin/src/main/scala/com/nvidia/spark/rapids/basicPhysicalOperators.scala)
when `spark.rapids.sql.projectAstJitEnabled=true`. The project expression list is
passed through `GpuAstJitExpression.wrapProjectExpressions`.

ANSI add should be controlled by the same experimental Project AST JIT switch as
regular add. No separate ANSI-add-specific toggle is needed for the first patch:
when `spark.rapids.sql.projectAstJitEnabled=true`, both supported non-ANSI add
and supported ANSI add are eligible for Project AST JIT; when it is false, both
stay on their existing non-JIT paths.

`GpuAddBase` currently opts into AST JIT only for non-ANSI integral add:

```scala
override protected def astJitCompatible: Boolean =
  !failOnError && (dataType == IntegerType || dataType == LongType)
```

That lives in
[`arithmetic.scala`](sql-plugin/src/main/scala/org/apache/spark/sql/rapids/arithmetic.scala).
When `failOnError=true`, `GpuAddBase.hasSideEffects` is true for basic numeric
overflow-checking types, and the regular columnar path evaluates a normal cuDF
add followed by `AddOverflowChecks.basicOpOverflowCheck`.

The generic AST conversion for `CudfBinaryExpression` emits a plain cuDF AST
`BinaryOperation(BinaryOperator.ADD, ...)`. That operation is not sufficient for
ANSI semantics because it does not represent an overflow-checking add.

`CudfBinaryExpression.selfSupportsAstJit` is final and is driven by
`astOperator.isDefined && astJitCompatible`, so ANSI add support must be
reflected in `GpuAddBase.astJitCompatible`. Overriding `convertToAst` alone is
not enough to make the expression eligible. The existing
`spark.rapids.sql.projectAstJitEnabled` flag then controls whether that
eligibility is used by `GpuProject`.

The matching cuDF Java AST API in the sibling cudf checkout already exposes
overflow-aware JIT operations:

- `ai.rapids.cudf.ast.JitOperation`
- `ai.rapids.cudf.ast.JitOperator.ADD_OVERFLOW`
- `ai.rapids.cudf.ast.JitErrorPolicy.PROPAGATE`
- `ai.rapids.cudf.ast.JitErrorPolicy.NULLIFY`

## Non-Goals for the First Patch

- Do not modify `cudf-spark-jni` or `thirdparty/cudf` for this Spark-side
  investigation unless later evidence shows a cuDF/JNI bug.
- Do not route this through the existing JNI/LTO checked-add path. That may be
  useful independently, but it is not Project AST JIT.
- Do not add `ByteType` or `ShortType` ANSI add support in the first patch. The
  experimental support boundary is explicitly `IntegerType` and `LongType`.
- Do not add decimal ANSI add support in the first patch. Decimal overflow and
  precision checks need a separate design around `CHECK_PRECISION`, rescale, and
  Spark decimal semantics.
- Do not enable aggregate expression AST JIT in the same change. A query such as
  `SELECT MAX(a + a) FROM mytable` currently goes through aggregate pre-step
  binding, not a plain `GpuProjectExec` expression list.

## Proposed Implementation

### 1. Use the existing Project AST JIT flag

The effective eligibility rule is:

- `spark.rapids.sql.projectAstJitEnabled=false`: no Project AST JIT, including
  ANSI add.
- `spark.rapids.sql.projectAstJitEnabled=true`: supported non-ANSI add and
  supported ANSI add are both eligible for Project AST JIT.
- `spark.sql.ansi.enabled=true` and resolved result type is `IntegerType` or
  `LongType`: ANSI add may be represented as a checked cuDF JIT operation.

Do not add a separate `RapidsConf` entry for ANSI add. Users who want to disable
this experimental path can disable `spark.rapids.sql.projectAstJitEnabled`,
which also disables regular add AST JIT.

### 2. Add an ANSI-integral predicate in `GpuAddBase`

Add a small helper in `GpuAddBase` to define the initial supported surface:

```scala
private def isProjectAstJitAddSupportedType: Boolean =
  dataType == IntegerType || dataType == LongType

private def isProjectAstJitAnsiAdd: Boolean =
  failOnError && isProjectAstJitAddSupportedType
```

Then relax `astJitCompatible` to include both non-ANSI and ANSI integral add:

```scala
override protected def astJitCompatible: Boolean =
  isProjectAstJitAddSupportedType
```

Keeping the supported type test narrow preserves the current AST JIT coverage and
avoids accidentally opting in decimal, floating point, and interval behavior.
It also documents that the experimental ANSI surface is `IntegerType` and
`LongType` only.

### 3. Override `GpuAddBase.convertToAst`

For ANSI integral add, emit a cuDF JIT operation instead of the generic
`BinaryOperation(ADD, ...)`:

```scala
override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
  val leftAst = left.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns)
  val rightAst = right.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns)

  if (isProjectAstJitAnsiAdd) {
    new ast.JitOperation(
      ast.JitOperator.ADD_OVERFLOW,
      ast.JitErrorPolicy.PROPAGATE,
      leftAst,
      rightAst)
  } else {
    new ast.BinaryOperation(ast.BinaryOperator.ADD, leftAst, rightAst)
  }
}
```

This will require importing or qualifying `JitOperation`, `JitOperator`, and
`JitErrorPolicy` from `ai.rapids.cudf.ast`.

The guard must match the actual supported AST JIT types. Do not rely directly on
`GpuAnsi.needBasicOpOverflowCheck(dataType)`, because that helper may include
types that Project AST JIT is not ready to support.

### 4. Preserve legacy AST behavior unless explicitly expanded

`AddRuleMeta.tagSelfForAst` currently rejects ANSI add for the legacy AST path.
That should stay unless the patch also proves legacy non-JIT AST can represent
the same overflow semantics.

Project AST JIT uses `GpuExpression.supportsAstJit` and
`GpuAstJitExpression.wrapProjectExpressions`, so it can be enabled independently
from legacy Project AST.

### 5. Verify side-effect handling

ANSI add can throw, so it is treated as side-effecting. Before landing the
change, audit the tiered project planner and expression grouping behavior:

- Do not cross an AST JIT boundary through side-effecting expressions.
- Do not use common-subexpression elimination in a way that removes or duplicates
  a required overflow check.
- Make an explicit first-patch decision for multi-output groups containing ANSI
  add. Either keep them grouped and test the query-level overflow behavior, or
  conservatively force ANSI-overflow groups to single-output evaluation while the
  feature is experimental.

Relevant code:

- [`GpuAstJitProjectPlanner.scala`](sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuAstJitProjectPlanner.scala)
- [`GpuAstJitExpression.scala`](sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuAstJitExpression.scala)
- [`GpuBoundAttribute.scala`](sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuBoundAttribute.scala)

If the existing side-effect handling is too broad, a conservative first patch can
allow ANSI `GpuAdd` only as a top-level AST JIT project expression and avoid
combining it across tier boundaries.

### 6. Verify exception mapping

The regular `GpuAdd` path throws Spark-style arithmetic overflow errors through
`RapidsErrorUtils.arithmeticOverflowError`. The cuDF AST JIT path may throw a
lower-level `CudfException` from `AstJitProgram.computeTable`.

Run an overflow case and compare:

```sql
SELECT 2147483647 + a FROM values(1) AS t(a)
```

If the AST JIT path exposes the wrong exception class or message, add narrowly
scoped translation around Project AST JIT evaluation. At minimum, the
experimental path must fail on overflow rather than silently producing wrapped
values. If exact Spark error parity is deferred, document that limitation and
keep the tests focused on the overflow failure and plan shape.

Avoid translating all `CudfException`s, because unrelated JIT
compilation/evaluation failures should not be reported as Spark arithmetic
overflow.

Potential approaches:

- Track whether a `GpuAstJitExpression` group contains ANSI overflow operations.
- On evaluation failure for such a group, translate only known cuDF evaluation
  overflow failures into `RapidsErrorUtils.arithmeticOverflowError`.
- Leave non-overflow JIT failures unchanged.

## Tests

### Unit Tests

Update
[`GpuProjectAstJitSuite.scala`](tests/src/test/scala/com/nvidia/spark/rapids/GpuProjectAstJitSuite.scala).

Suggested changes:

- Replace the current "excludes ANSI and floating point arithmetic" expectation
  with separate tests:
  - With `spark.rapids.sql.projectAstJitEnabled=true`, ANSI
    `GpuAdd(IntegerType)` is wrapped in `GpuAstJitExpression`.
  - With `spark.rapids.sql.projectAstJitEnabled=true`, ANSI `GpuAdd(LongType)`
    is wrapped in `GpuAstJitExpression`.
  - With `spark.rapids.sql.projectAstJitEnabled=false`, ANSI
    `GpuAdd(IntegerType)` and `GpuAdd(LongType)` remain on the regular path.
  - `ByteType` and `ShortType` ANSI add remain excluded for this experimental
    patch.
  - Floating point arithmetic remains excluded.
  - Decimal and interval add remain excluded.
- Add a test that inspects the converted AST string or mocked AST conversion to
  prove ANSI add uses `ADD_OVERFLOW`, while non-ANSI add still uses plain `ADD`.
- Add a tiering test with an ANSI add feeding another expression to verify the
  planner respects side-effect boundaries.

Targeted command:

```bash
mvn package -pl tests -am \
  -Dbuildver=350 \
  -Dcuda.version=cuda13 \
  -DwildcardSuites=com.nvidia.spark.rapids.GpuProjectAstJitSuite
```

### Execution Tests

Add an integration or Scala execution test that runs with:

```scala
spark.conf.set("spark.sql.ansi.enabled", "true")
spark.conf.set("spark.rapids.sql.projectAstJitEnabled", "true")
```

Cases to cover:

- Non-overflow `INT` add matches CPU.
- Non-overflow `LONG` add matches CPU.
- Overflow `INT` add throws.
- Overflow `LONG` add throws.
- Null input rows remain null and do not throw.
- The executed GPU plan contains `GpuAstJitExpression` or equivalent AST JIT
  plan evidence.
- With Project AST JIT disabled, the same ANSI add query still runs on GPU
  through the existing non-JIT `GpuAdd` path and does not contain
  `GpuAstJitExpression`.
- A projection with multiple ANSI adds in the same `GpuProject` either preserves
  the chosen multi-output behavior or proves that ANSI-overflow expressions are
  intentionally evaluated as single-output JIT groups.

Keep the collected result small. A large `collect()` can hit
`spark.driver.maxResultSize` and obscure the actual AST JIT behavior.

## Aggregate Follow-Up

This plan only makes plain `GpuProjectExec` expressions eligible. For a query
like:

```sql
SELECT MAX(a + a) FROM mytable
```

the add may be evaluated as an aggregate pre-step expression. Today
`GpuAggregateExec` binds pre-step and post-step expressions through
`GpuBindReferences.bindGpuReferencesTiered` without enabling Project AST JIT.

Supporting that shape would require a follow-up design, for example:

- Enable AST JIT for eligible aggregate pre-step expressions, with the same
  side-effect checks.
- Or introduce/materialize a `GpuProjectExec` before the aggregate when doing so
  is semantically and physically acceptable.

## Rollout Strategy

1. Land plain Project AST JIT support for ANSI `IntegerType` and `LongType`
   `GpuAdd`, controlled by the existing `spark.rapids.sql.projectAstJitEnabled`
   flag.
2. Validate exception behavior and plan evidence under ANSI mode.
3. Document that `ByteType` and `ShortType` are intentionally out of scope for
   the experimental milestone.
4. Decide whether to expand to `ByteType` and `ShortType`.
5. Design decimal ANSI support separately.
6. Design aggregate pre-step AST JIT separately.

## Open Questions

- Does cuDF AST JIT `ADD_OVERFLOW` with `PROPAGATE` produce an exception that
  Spark RAPIDS can expose directly, or do we need translation?
- Should ANSI overflow-aware AST JIT expressions be allowed in multi-output JIT
  groups, or should the first patch force them to compile/evaluate separately?
- Is `ByteType` and `ShortType` support needed for Spark-facing add, or does
  Spark type coercion make `IntegerType` and `LongType` sufficient for the first
  useful implementation?
- Is the shared Project AST JIT flag sufficient long-term, or would a narrower
  ANSI-add-specific escape hatch be useful after the behavior is proven?
