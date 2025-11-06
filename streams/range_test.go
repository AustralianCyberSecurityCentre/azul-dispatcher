package streams

import "testing"

func TestRange(t *testing.T) {
	// none/empty range
	r, err := parseRange("")
	if err != nil {
		t.Errorf("Unexpected error for valid range got: %v", err)
	}
	if r.start != 0 || r.length != -1 {
		t.Errorf("Expected full range for empty but got: %v", r)
	}
	// bad units
	r, err = parseRange("franks=2:10")
	if err == nil {
		t.Errorf("Expected error for bad unit type but got: %v", r)
	}
	// empty range
	r, err = parseRange("bytes=")
	if err == nil {
		t.Errorf("Expected error for empty range but got: %v", r)
	}
	// invalid values
	r, err = parseRange("bytes=a-f")
	if err == nil {
		t.Errorf("Expected error for invalid range but got: %v", r)
	}
	// invalid values
	r, err = parseRange("bytes=10-f")
	if err == nil {
		t.Errorf("Expected error for invalid range but got: %v", r)
	}
	// invalid range
	r, err = parseRange("bytes=-")
	if err == nil {
		t.Errorf("Expected error for invalid range but got: %v", r)
	}
	// valid with implied end
	r, err = parseRange("bytes=100-")
	if err != nil {
		t.Errorf("Unexpected error for valid range got: %v", err)
	}
	if r.start != 100 || r.length != -1 {
		t.Errorf("Expected valid range but got: %v", r)
	}
	// valid with relative start
	r, err = parseRange("bytes=-100")
	if err != nil {
		t.Errorf("Unexpected error for valid range got: %v", err)
	}
	if r.start != -100 || r.length != 100 {
		t.Errorf("Expected valid range but got: %v", r)
	}
	// valid with end
	r, err = parseRange("bytes=100-200")
	if err != nil {
		t.Errorf("Unexpected error for valid range got: %v", err)
	}
	if r.start != 100 || r.length != 101 {
		t.Errorf("Expected valid range but got: %v", r)
	}
	// invalid range
	r, err = parseRange("bytes=-100-200")
	if err == nil {
		t.Errorf("Expected error for invalid range but got: %v", r)
	}
	// invalid range
	r, err = parseRange("bytes=200-100")
	if err == nil {
		t.Errorf("Expected error for invalid range but got: %v", r)
	}
	// valid range
	r, err = parseRange("bytes=2-2")
	if err != nil {
		t.Errorf("Unexpected error for valid range got: %v", err)
	}
	if r.start != 2 || r.length != 1 {
		t.Errorf("Expected valid range but got: %v", r)
	}
}
