package redisearch

import "testing"

// TestP3dParserASTD2 verifies the D2 parser produces AndNode(alpha, OrNode(beta,gamma))
// for "alpha beta | gamma" (| binds tighter than space).
func TestP3dParserASTD2(t *testing.T) {
	p := NewExpressionParser("alpha beta | gamma")
	p.SetDialect(2)
	node, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	// Expect AndNode{TermNode(alpha), OrNode{TermNode(beta), TermNode(gamma)}}
	and, ok := node.(*AndNode)
	if !ok {
		t.Fatalf("D2 top should be AndNode, got %T", node)
	}
	if _, ok := and.Left.(*TermNode); !ok {
		t.Fatalf("D2 and.left should be TermNode, got %T", and.Left)
	}
	or, ok := and.Right.(*OrNode)
	if !ok {
		t.Fatalf("D2 and.right should be OrNode, got %T (full: %#v)", and.Right, node)
	}
	if or.Left == nil || or.Right == nil {
		t.Fatalf("D2 OrNode missing children: %#v", or)
	}
	t.Logf("D2 AST: %#v", node)
}
