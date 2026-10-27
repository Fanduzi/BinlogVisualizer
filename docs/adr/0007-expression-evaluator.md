# Generated-column expressions have one parser and one evaluator

`internal/analyzer/flashback_eval.go` owns the expression evaluator. It parses an expression once into a tree, cached by its text, and evaluates that tree in one of two domains. `evalGenExpr(expr, env)` covers integers, text and JSON. `evalDecExprInc(expr, env, inc)` does exact DECIMAL arithmetic under `div_precision_increment`. The DECIMAL domain rejects any node outside its subset: `->`/`->>`, string literals, COLLATE, functions other than MOD, and MOD with an argument count other than 2. The value rules (`genVal`, `decVal`, `applyGenAdd`, `applyDecMul`, `evalGenCall`, `applyMulOp`) stay where they were.

This replaces three recursive-descent copies of the same grammar:

- `parseGen*`, the general domain;
- `parseDec*`, the DECIMAL domain;
- `evalIntExpr`/`parse*Expr` in `flashback_schema.go`, the integer domain. Only tests called it.

## Why

A change to the grammar had to be made two or three times. Nothing checked that the copies still agreed. The integer copy was dead, so a test of it proved nothing about production.

## Considered options

- Keep the copies and add a cross-check test. That still leaves three grammars to edit, plus a test whose only job is to keep them in step.
- Make DECIMAL a mode inside the general evaluator. This mixes two value types inside every apply step. One tree with two walks keeps each domain's arithmetic in one place.

## Consequences

- An expression is parsed once per process and not once per row and domain.
- Evaluation clones literal values, so a caller cannot change the cached tree.
- The tests in `flashback_eval_test.go` exercise the interface. The integer-assignment cases now run through `evalGenExpr`.
- Flashback output is unchanged byte for byte. Analyze does not call this module.

## Later fixes in this module

- #237: a JSON array index on a value that is not an array autowraps it, as in MySQL 8.0. `[0]` returns the value itself, and any other index is missing (NULL).
- #238: in the DECIMAL walk, DIV, `%` and MOD now accept operands that have a fraction. Before, such an operand sent the expression to the general evaluator, which divides exactly. Each `/` stays rounded the way MySQL rounds it:
  - DIV truncates the quotient toward zero; a quotient outside BIGINT is not claimed.
  - `%` is exact and takes the larger operand scale.
  - A zero divisor gives NULL whatever the dividend is.
  - An arithmetic result of zero is a plain 0 with scale 0 (MySQL's `decimal_make_zero`), and a zero dividend of `/` gives 0, so a later `/` is sized the way MySQL sizes it.
  - Operands over 60 digits still go to the general evaluator.

  A later `/` whose scale goes past 30 stays unverified, as before.
- ADR 0020: a COLLATE clause is unverified in both domains. The general domain used to parse it and then ignore it, so `UPPER('i' COLLATE utf8mb4_tr_0900_ai_ci)` came back as `I`. That disagrees with MySQL. The walk now returns unverified whenever a node carries COLLATE.
- ADR 0020: signed integer `+`, `-`, `*`, and `DIV` whose result is outside BIGINT are unverified. MySQL raises `ERROR 1690`. Two integer-typed operands stay on that arithmetic inside the DECIMAL domain; a DECIMAL operand uses exact decimal arithmetic and can exceed BIGINT.
