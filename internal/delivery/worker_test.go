package delivery

import (
	"testing"
	"time"
)

func TestRetryAfterBounded(t *testing.T) {
	now := time.Now()
	for _, header := range []string{"999999", "999999999999999999999999999999999", "-4", "garbage", now.Add(time.Hour).UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")} {
		d := RetryDelay(2, header, now)
		if d < 2*time.Second || d > 60*time.Second {
			t.Fatalf("unbounded delay %s for %q", d, header)
		}
	}
	if RetryDelay(1, "45", now) != 45*time.Second {
		t.Fatal("Retry-After not respected")
	}
}

func TestTargetValidation(t *testing.T) {
	for _, u := range []string{"file:///etc/passwd", "http://example.com", "https://user:pass@example.com", "https://example.com/#fragment"} {
		if ValidateTargets(map[string]Target{"alpha": {URL: u, Secret: "12345678901234567890123456789012"}}, false) == nil {
			t.Fatalf("accepted %s", u)
		}
	}
	if err := ValidateTargets(map[string]Target{"alpha": {URL: "https://example.com/webhook", Secret: "12345678901234567890123456789012"}}, false); err != nil {
		t.Fatal(err)
	}
}
