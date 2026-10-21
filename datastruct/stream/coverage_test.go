package stream

import (
	"testing"
	"time"
)

func addSample(t *testing.T, s *Stream, id string) StreamID {
	t.Helper()
	sid, err := s.Add(id, map[string]string{"f": id}, nil)
	if err != nil {
		t.Fatalf("Add(%s): %v", id, err)
	}
	return sid
}

func TestMetadataSettersAndInfo(t *testing.T) {
	s := NewStream()
	id1 := addSample(t, s, "1-1")
	addSample(t, s, "1-2")
	s.SetLastID(StreamID{Timestamp: 9, Sequence: 9})
	s.SetEntriesAdded(42)
	s.SetMaxDeletedID(StreamID{Timestamp: 3, Sequence: 1})

	if s.GetLastID() != (StreamID{Timestamp: 9, Sequence: 9}) {
		t.Fatalf("GetLastID = %v", s.GetLastID())
	}
	if s.GetEntriesAdded() != 42 {
		t.Fatalf("GetEntriesAdded = %d", s.GetEntriesAdded())
	}
	if s.GetMaxDeletedID() != (StreamID{Timestamp: 3, Sequence: 1}) {
		t.Fatalf("GetMaxDeletedID = %v", s.GetMaxDeletedID())
	}
	info := s.GetInfo()
	if info["length"] != 2 || info["entries-added"] != int64(42) {
		t.Fatalf("Info = %#v", info)
	}
	if info["first-entry"] == nil || info["last-entry"] == nil {
		t.Fatalf("Info missing first/last entry: %#v", info)
	}
	if id1.String() != "1-1" {
		t.Fatalf("sample id = %s", id1.String())
	}
}

func TestGroupsClaimAutoClaimAndDeleteEx(t *testing.T) {
	s := NewStream()
	id1 := addSample(t, s, "1-1")
	id2 := addSample(t, s, "1-2")
	if err := s.CreateGroup("g", "0"); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	group, err := s.GetGroup("g")
	if err != nil {
		t.Fatalf("GetGroup: %v", err)
	}
	group.SetEntriesRead(1)
	group.RLock()
	group.RUnlock()
	if len(s.GetGroups()) != 1 {
		t.Fatalf("GetGroups = %d", len(s.GetGroups()))
	}

	entries, err := s.Claim("g", "c1", 0, []StreamID{id1}, &ClaimOptions{Force: true})
	if err != nil || len(entries) != 1 {
		t.Fatalf("Claim = %d/%v", len(entries), err)
	}
	if _, err := s.Claim("nope", "c1", 0, []StreamID{id1}, nil); err == nil {
		t.Fatal("Claim on missing group should fail")
	}
	// Force-create a PEL entry for AutoClaim.
	if _, err := s.Claim("g", "c1", 0, []StreamID{id2}, &ClaimOptions{Force: true}); err != nil {
		t.Fatalf("Claim force: %v", err)
	}
	group.Lock()
	if pe := group.Pending[id2]; pe != nil {
		pe.DeliveryTime = time.Now().Add(-time.Minute)
	}
	group.Unlock()
	claimed, deleted, _, err := s.AutoClaim("g", "c2", 0, StreamID{}, 10, nil)
	if err != nil || len(claimed)+len(deleted) == 0 {
		t.Fatalf("AutoClaim = %d/%d/%v", len(claimed), len(deleted), err)
	}
	if _, _, _, err := s.AutoClaim("nope", "c2", 0, StreamID{}, 10, nil); err == nil {
		t.Fatal("AutoClaim on missing group should fail")
	}

	// ACKED keeps entries still referenced by a PEL; DELREF removes them and
	// clears the PEL references.
	results := s.DeleteExResults([]StreamID{id1}, "ACKED")
	if len(results) != 1 || results[0] != 2 {
		t.Fatalf("DeleteExResults ACKED = %v, want [2]", results)
	}
	if n := s.DeleteEx([]StreamID{id1}, "DELREF"); n != 1 {
		t.Fatalf("DeleteEx DELREF = %d, want 1", n)
	}
	if s.GetEntry(id1) != nil {
		t.Fatal("DELREF should delete the entry")
	}
	if n := s.DeleteEx([]StreamID{StreamID{Timestamp: 99}}, "KEEPREF"); n != 0 {
		t.Fatalf("DeleteEx missing = %d", n)
	}

	if _, err := group.DeleteConsumer("c1"); err != nil {
		t.Fatalf("DeleteConsumer: %v", err)
	}
	if _, err := group.DeleteConsumer("c1"); err != ErrNoSuchConsumer {
		t.Fatalf("DeleteConsumer missing = %v", err)
	}
	if err := s.DestroyGroup("g"); err != nil {
		t.Fatalf("DestroyGroup: %v", err)
	}
	if err := s.DestroyGroup("g"); err != ErrNoSuchGroup {
		t.Fatalf("DestroyGroup missing = %v", err)
	}
}

func TestIdmpRecordLookupAndEviction(t *testing.T) {
	s := NewStream()
	s.SetIdmpConfig(StreamIdmpConfig{Duration: time.Hour, MaxSize: 2})
	if got := s.IdmpConfig(); got.MaxSize != 2 || got.Duration != time.Hour {
		t.Fatalf("IdmpConfig = %+v", got)
	}
	s.RecordIdmp("p", "i1", StreamID{Timestamp: 1, Sequence: 1})
	if id, ok := s.LookupIdmp("p", "i1"); !ok || id.Timestamp != 1 {
		t.Fatalf("LookupIdmp = %v/%v", id, ok)
	}
	// MaxSize=2: third record evicts the oldest.
	s.RecordIdmp("p", "i2", StreamID{Timestamp: 2, Sequence: 2})
	s.RecordIdmp("p", "i3", StreamID{Timestamp: 3, Sequence: 3})
	if len(s.idmp["p"]) > 2 {
		t.Fatalf("maxsize not enforced: %d", len(s.idmp["p"]))
	}
	s.SetIdmpConfig(StreamIdmpConfig{Duration: time.Millisecond})
	s.RecordIdmp("p", "old", StreamID{Timestamp: 4, Sequence: 4})
	time.Sleep(5 * time.Millisecond)
	if _, ok := s.LookupIdmp("p", "old"); ok {
		t.Fatal("expired IDMP record should miss")
	}
	if _, ok := s.LookupIdmp("missing", "x"); ok {
		t.Fatal("unknown producer should miss")
	}
}
