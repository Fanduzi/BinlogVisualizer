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
