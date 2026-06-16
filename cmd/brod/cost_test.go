package main

import "testing"

func TestTeamFlagsParsingAndMatch(t *testing.T) {
	var tf teamFlags
	if err := tf.Set(`dev\.=Platform`); err != nil {
		t.Fatal(err)
	}
	if err := tf.Set(`events|clicks=Data`); err != nil {
		t.Fatal(err)
	}
	// ordered, first match wins; no match -> unassigned
	cases := map[string]string{
		"dev.orders":   "Platform",
		"events.raw":   "Data",
		"clicks":       "Data",
		"legacy.dump":  "unassigned",
	}
	for topic, want := range cases {
		if got := tf.teamFor(topic); got != want {
			t.Errorf("teamFor(%q) = %q, want %q", topic, got, want)
		}
	}
}

func TestTeamFlagsRejectsBadInput(t *testing.T) {
	var tf teamFlags
	if err := tf.Set("noequalsign"); err == nil {
		t.Error("expected error for missing '='")
	}
	if err := tf.Set("(=Bad"); err == nil {
		t.Error("expected error for invalid regex")
	}
}
