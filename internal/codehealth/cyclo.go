package codehealth

import (
	"go/ast"
	"go/token"
)

// complexities returns the cyclomatic complexity of each function declared
// in f, keyed "Func" or "Recv.Func". It counts like gocyclo: 1, plus one per
// if, for, range, non-default case, non-default select case, && and ||.
// Function literals count toward the function that contains them.
func complexities(f *ast.File) map[string]int {
	out := map[string]int{}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		name := fn.Name.Name
		if recv := receiverName(fn); recv != "" {
			name = recv + "." + name
		}
		if c := complexity(fn); c > out[name] {
			out[name] = c
		}
	}
	return out
}

func complexity(fn *ast.FuncDecl) int {
	c := 1
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt:
			c++
		case *ast.CaseClause:
			if n.List != nil {
				c++
			}
		case *ast.CommClause:
			if n.Comm != nil {
				c++
			}
		case *ast.BinaryExpr:
			if n.Op == token.LAND || n.Op == token.LOR {
				c++
			}
		}
		return true
	})
	return c
}

func receiverName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return ""
	}
	t := fn.Recv.List[0].Type
	for {
		switch x := t.(type) {
		case *ast.StarExpr:
			t = x.X
		case *ast.IndexExpr:
			t = x.X
		case *ast.IndexListExpr:
			t = x.X
		case *ast.ParenExpr:
			t = x.X
		case *ast.Ident:
			return x.Name
		default:
			return ""
		}
	}
}
