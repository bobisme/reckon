#!/bin/bash
# Benchmark script for warm cache performance
#
# This script measures reckon's warm-cache performance using hyperfine.
# A warm cache run should complete in < 200ms on a modern machine.
#
# Prerequisites:
#   - hyperfine (brew install hyperfine or apt install hyperfine)
#   - cargo build --release already run
#
# Usage:
#   ./scripts/bench-warm.sh
#
# Regression detection:
#   - Fails if mean time exceeds 300ms (severe regression)
#   - Warns if mean time exceeds 200ms (performance target)

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Check if hyperfine is installed
if ! command -v hyperfine &> /dev/null; then
    echo -e "${RED}Error: hyperfine not found. Install it with:${NC}"
    echo "  macOS: brew install hyperfine"
    echo "  Ubuntu/Debian: apt install hyperfine"
    echo "  Or from: https://github.com/sharkdp/hyperfine"
    exit 1
fi

# Ensure we're using the release binary
cd "$PROJECT_ROOT"
echo -e "${YELLOW}Building release binary...${NC}"
cargo build --release --bin reckon 2>&1 | grep -E "^(Compiling|Finished|error)" || true

echo -e "${YELLOW}Running benchmark with warm cache...${NC}"
echo "  - 3 warmup iterations"
echo "  - 10 benchmark runs"
echo "  - Using cached index at ~/.cache/reckon/index.sqlite"
echo ""

# Run benchmark with hyperfine
# - 3 warmup runs as specified in bone
# - 10 actual runs for statistical significance
# - Run offline to avoid network variance in pricing refresh
BENCH_CMD="$PROJECT_ROOT/target/release/reckon --offline"

# Run hyperfine and capture output
HYPERFINE_OUTPUT=$(hyperfine \
  --warmup 3 \
  --min-runs 10 \
  "$BENCH_CMD" 2>&1)

echo "$HYPERFINE_OUTPUT"

# Parse the mean time from hyperfine output
# Output format: "  Time (mean ± σ):      1.915 s ±  0.023 s"
MEAN_LINE=$(echo "$HYPERFINE_OUTPUT" | grep "Time (mean" || true)

if [ -z "$MEAN_LINE" ]; then
    echo -e "${RED}Error: Could not parse benchmark results${NC}"
    exit 1
fi

# Extract mean time in seconds
# The mean is the 4th space-separated field on the line with "Time (mean"
# e.g., from "  Time (mean ± σ):      1.915 s ±  0.023 s" we want "1.915"
MEAN_SECONDS=$(echo "$MEAN_LINE" | sed 's/.*Time[^0-9]*\([0-9.]*\).*/\1/')

# Convert to milliseconds using awk to avoid printf issues with unicode
MEAN_MS=$(echo "$MEAN_SECONDS" | awk '{print int($1 * 1000 + 0.5)}')

echo ""
echo -e "${GREEN}=== Benchmark Results ===${NC}"
echo "Mean time: ${MEAN_MS}ms"
echo "Target: < 200ms (warn if > 200ms, fail if > 300ms)"
echo ""

# Check thresholds
if [ "$MEAN_MS" -gt 300 ]; then
    echo -e "${RED}REGRESSION DETECTED: Severe slowdown${NC}"
    echo "Mean execution time (${MEAN_MS}ms) exceeds 300ms threshold"
    exit 1
elif [ "$MEAN_MS" -gt 200 ]; then
    echo -e "${YELLOW}Warning: Below performance target${NC}"
    echo "Mean execution time (${MEAN_MS}ms) exceeds 200ms target"
    echo "This is expected during development; optimize readers and cache usage"
    exit 0
else
    echo -e "${GREEN}Performance target met!${NC}"
    exit 0
fi
