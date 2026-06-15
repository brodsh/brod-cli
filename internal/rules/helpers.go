package rules

import (
	"math"

	"github.com/brodsh/brod-cli/internal/cost"
)

// round2 rounds a euro/number to 2 decimals for stable display + JSON.
func round2(f float64) float64 { return math.Round(f*100) / 100 }

// monthlyGB converts a bytes/sec rate to GB ingested per 30-day month.
func monthlyGB(bytesPerSec float64) float64 { return cost.MonthlyGB(bytesPerSec) }

func maxI64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minF(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
