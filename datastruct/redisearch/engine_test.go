package redisearch

import (
	"testing"
)

func TestMultiFieldGroupBy(t *testing.T) {
	engine := NewRediSearchEngine(&EngineConfig{
		Name: "test_idx",
	})
	
	// Create schema
	fields := []*Field{
		{Name: "category", Type: FieldTypeText, Sortable: true},
		{Name: "region", Type: FieldTypeText, Sortable: true},
		{Name: "amount", Type: FieldTypeNumeric, Sortable: true},
	}
	if err := engine.CreateIndex(fields); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	
	// Add documents
	docs := []struct {
		id     string
		fields map[string]interface{}
	}{
		{"doc1", map[string]interface{}{"category": "A", "region": "North", "amount": 100.0}},
		{"doc2", map[string]interface{}{"category": "A", "region": "North", "amount": 200.0}},
		{"doc3", map[string]interface{}{"category": "A", "region": "South", "amount": 150.0}},
		{"doc4", map[string]interface{}{"category": "B", "region": "North", "amount": 300.0}},
		{"doc5", map[string]interface{}{"category": "B", "region": "South", "amount": 250.0}},
	}
	
	for _, doc := range docs {
		if err := engine.AddDocument(doc.id, doc.fields, 1.0, nil); err != nil {
			t.Fatalf("Failed to add document: %v", err)
		}
	}
	
	// Test multi-field GROUPBY
	req := &AggregationRequest{
		Query:   "*",
		GroupBy: []string{"category", "region"},
		Reduce: []Reducer{
			{Function: "SUM", Field: "amount", As: "total"},
			{Function: "COUNT", As: "count"},
		},
		Limit: 10,
	}
	
	result, err := engine.Aggregate(req)
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}
	
	// Should have 4 groups: (A,North), (A,South), (B,North), (B,South)
	if result.Total != 4 {
		t.Errorf("Expected 4 groups, got %d", result.Total)
	}
	
	// Verify group totals
	expectedTotals := map[string]float64{
		"A|$North": 300.0, // 100 + 200
		"A|$South": 150.0,
		"B|$North": 300.0,
		"B|$South": 250.0,
	}
	expectedCounts := map[string]int{
		"A|$North": 2,
		"A|$South": 1,
		"B|$North": 1,
		"B|$South": 1,
	}
	
	for _, group := range result.Groups {
		key, ok := group.By.(string)
		if !ok {
			t.Errorf("Group key is not a string: %v", group.By)
			continue
		}
		
		expectedTotal, exists := expectedTotals[key]
		if !exists {
			t.Errorf("Unexpected group key: %s", key)
			continue
		}
		
		total, ok := group.Fields["total"].(float64)
		if !ok {
			t.Errorf("Group %s: total is not float64: %v", key, group.Fields["total"])
			continue
		}
		
		if total != expectedTotal {
			t.Errorf("Group %s: expected total %.2f, got %.2f", key, expectedTotal, total)
		}
		
		count, ok := group.Fields["count"].(int)
		if !ok {
			t.Errorf("Group %s: count is not int: %v", key, group.Fields["count"])
			continue
		}
		
		if count != expectedCounts[key] {
			t.Errorf("Group %s: expected count %d, got %d", key, expectedCounts[key], count)
		}
		
		// Verify individual field values are stored
		if group.Fields["category"] == nil {
			t.Errorf("Group %s: category field not stored", key)
		}
		if group.Fields["region"] == nil {
			t.Errorf("Group %s: region field not stored", key)
		}
	}
}

func TestHavingClause(t *testing.T) {
	engine := NewRediSearchEngine(&EngineConfig{
		Name: "test_having_idx",
	})
	
	// Create schema
	fields := []*Field{
		{Name: "category", Type: FieldTypeText, Sortable: true},
		{Name: "amount", Type: FieldTypeNumeric, Sortable: true},
	}
	if err := engine.CreateIndex(fields); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	
	// Add documents - Each category will have:
	// A: sum = 300 (100 + 200)
	// B: sum = 125 (50 + 75)
	// C: sum = 500
	docs := []struct {
		id     string
		fields map[string]interface{}
	}{
		{"doc1", map[string]interface{}{"category": "A", "amount": 100.0}},
		{"doc2", map[string]interface{}{"category": "A", "amount": 200.0}},
		{"doc3", map[string]interface{}{"category": "B", "amount": 50.0}},
		{"doc4", map[string]interface{}{"category": "B", "amount": 75.0}},
		{"doc5", map[string]interface{}{"category": "C", "amount": 500.0}},
	}
	
	for _, doc := range docs {
		if err := engine.AddDocument(doc.id, doc.fields, 1.0, nil); err != nil {
			t.Fatalf("Failed to add document: %v", err)
		}
	}
	
	// Test HAVING with > operator
	req := &AggregationRequest{
		Query:   "*",
		GroupBy: []string{"category"},
		Reduce: []Reducer{
			{Function: "SUM", Field: "amount", As: "total"},
		},
		Having: &HavingClause{
			Left:     "total",
			Operator: ">",
			Right:    200.0,
		},
		Limit: 10,
	}
	
	result, err := engine.Aggregate(req)
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}
	
	// Should have 2 groups: A (300) and C (500), B has only 125 so it's filtered out
	if result.Total != 2 {
		t.Errorf("Expected 2 groups after HAVING, got %d", result.Total)
	}
	
	// Verify the groups
	found := make(map[string]bool)
	for _, group := range result.Groups {
		key := group.By.(string)
		found[key] = true
		total := group.Fields["total"].(float64)
		if total <= 200 {
			t.Errorf("Group %s has total %.2f <= 200, should be filtered out", key, total)
		}
	}
	
	if !found["A"] || !found["C"] {
		t.Errorf("Expected groups A and C, got %v", found)
	}
	if found["B"] {
		t.Errorf("Group B should be filtered out")
	}
}

func TestHavingClauseWithEquals(t *testing.T) {
	engine := NewRediSearchEngine(&EngineConfig{
		Name: "test_having_eq_idx",
	})
	
	// Create schema
	fields := []*Field{
		{Name: "category", Type: FieldTypeText, Sortable: true},
		{Name: "amount", Type: FieldTypeNumeric, Sortable: true},
	}
	if err := engine.CreateIndex(fields); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	
	// Add documents
	// A: sum = 200 (100 + 100) = 200
	// B: sum = 400 (200 + 200) = 400
	// C: sum = 150
	docs := []struct {
		id     string
		fields map[string]interface{}
	}{
		{"doc1", map[string]interface{}{"category": "A", "amount": 100.0}},
		{"doc2", map[string]interface{}{"category": "A", "amount": 100.0}},
		{"doc3", map[string]interface{}{"category": "B", "amount": 200.0}},
		{"doc4", map[string]interface{}{"category": "B", "amount": 200.0}},
		{"doc5", map[string]interface{}{"category": "C", "amount": 150.0}},
	}
	
	for _, doc := range docs {
		if err := engine.AddDocument(doc.id, doc.fields, 1.0, nil); err != nil {
			t.Fatalf("Failed to add document: %v", err)
		}
	}
	
	// Test HAVING with = operator - filter for total = 200
	req := &AggregationRequest{
		Query:   "*",
		GroupBy: []string{"category"},
		Reduce: []Reducer{
			{Function: "SUM", Field: "amount", As: "total"},
		},
		Having: &HavingClause{
			Left:     "total",
			Operator: "=",
			Right:    200.0,
		},
		Limit: 10,
	}
	
	result, err := engine.Aggregate(req)
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}
	
	// Should have 1 group: A with total = 200
	if result.Total != 1 {
		t.Errorf("Expected 1 group with total = 200, got %d groups", result.Total)
		for _, g := range result.Groups {
			t.Logf("Group: %v, total: %v", g.By, g.Fields["total"])
		}
	}
	
	if result.Total > 0 {
		group := result.Groups[0]
		if group.By.(string) != "A" {
			t.Errorf("Expected group A, got %v", group.By)
		}
	}
}

func TestHavingClauseGreaterThanOrEqual(t *testing.T) {
	engine := NewRediSearchEngine(&EngineConfig{
		Name: "test_having_ge_idx",
	})
	
	// Create schema
	fields := []*Field{
		{Name: "category", Type: FieldTypeText, Sortable: true},
		{Name: "amount", Type: FieldTypeNumeric, Sortable: true},
	}
	if err := engine.CreateIndex(fields); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	
	// Add documents
	docs := []struct {
		id     string
		fields map[string]interface{}
	}{
		{"doc1", map[string]interface{}{"category": "A", "amount": 100.0}},
		{"doc2", map[string]interface{}{"category": "A", "amount": 100.0}}, // total = 200
		{"doc3", map[string]interface{}{"category": "B", "amount": 199.0}}, // total = 199
		{"doc4", map[string]interface{}{"category": "C", "amount": 250.0}}, // total = 250
	}
	
	for _, doc := range docs {
		if err := engine.AddDocument(doc.id, doc.fields, 1.0, nil); err != nil {
			t.Fatalf("Failed to add document: %v", err)
		}
	}
	
	// Test HAVING with >= operator
	req := &AggregationRequest{
		Query:   "*",
		GroupBy: []string{"category"},
		Reduce: []Reducer{
			{Function: "SUM", Field: "amount", As: "total"},
		},
		Having: &HavingClause{
			Left:     "total",
			Operator: ">=",
			Right:    200.0,
		},
		Limit: 10,
	}
	
	result, err := engine.Aggregate(req)
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}
	
	// Should have 2 groups: A (200) and C (250), B has 199 so it's filtered out
	if result.Total != 2 {
		t.Errorf("Expected 2 groups after HAVING >= 200, got %d", result.Total)
	}
}
