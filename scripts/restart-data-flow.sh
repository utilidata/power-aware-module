#!/bin/bash
# Helper script to restart the data flow (replay → exporter/db)
# Optionally switch datasets before restarting
#
# Usage:
#   ./scripts/restart-data-flow.sh                    # Use current dataset
#   ./scripts/restart-data-flow.sh sample2            # Switch to sample2-b200-static-powercap.csv
#   ./scripts/restart-data-flow.sh sample3            # Switch to sample3-b200-dynamic-powercap.csv

set -e

NAMESPACE="${NAMESPACE:-karman}"
SCENARIO="${1:-}"  # Optional first argument (scenario name without .csv)

# Map short names to full filenames
if [ -n "$SCENARIO" ]; then
    # If it already ends with .csv, use as-is
    if [[ "$SCENARIO" == *.csv ]]; then
        DATASET="$SCENARIO"
    # Map short scenario names to full filenames
    elif [[ "$SCENARIO" == "sample1" ]]; then
        DATASET="sample1-b200-no-powercap.csv"
    elif [[ "$SCENARIO" == "sample2" ]]; then
        DATASET="sample2-b200-static-powercap.csv"
    elif [[ "$SCENARIO" == "sample3" ]]; then
        DATASET="sample3-b200-dynamic-powercap.csv"
    elif [[ "$SCENARIO" == "sample4" ]]; then
        DATASET="sample4-b200-partial-dynamic-powercap.csv"
    # Also support full names without .csv
    elif [[ "$SCENARIO" == sample*-b200-* ]]; then
        DATASET="${SCENARIO}.csv"
    else
        echo "❌ Unknown scenario: $SCENARIO"
        echo ""
        echo "Available scenarios:"
        echo "  sample1  →  sample1-b200-no-powercap.csv"
        echo "  sample2  →  sample2-b200-static-powercap.csv"
        echo "  sample3  →  sample3-b200-dynamic-powercap.csv"
        echo "  sample4  →  sample4-b200-partial-dynamic-powercap.csv"
        echo ""
        echo "Or use full filename (with or without .csv)"
        exit 1
    fi
fi

echo "🔄 Restarting data flow in namespace: $NAMESPACE"
if [ -n "$DATASET" ]; then
    echo "📊 Switching to dataset: $DATASET"
fi
echo ""

# Optional: Switch dataset before restarting
if [ -n "$DATASET" ]; then
    echo "0️⃣  Updating dataset configuration..."
    helm upgrade karman-lab charts/karman-lab -n "$NAMESPACE" \
        --set replay.defaultDataset="$DATASET" \
        --reuse-values
    echo "   ✅ Dataset updated to $DATASET"
    echo ""
fi

# Restart all pods in parallel (init containers handle coordination)
echo "1️⃣  Restarting all data flow components..."
kubectl -n "$NAMESPACE" delete pod -l app=data-replay &
kubectl -n "$NAMESPACE" delete pod -l app=data-exporter &
kubectl -n "$NAMESPACE" delete pod -l app=data-db &
wait  # Wait for all delete commands to complete

echo "   Waiting for pods to restart..."
sleep 5
kubectl -n "$NAMESPACE" wait --for=condition=ready pod -l app=data-replay --timeout=90s
kubectl -n "$NAMESPACE" wait --for=condition=ready pod -l app=data-exporter --timeout=120s
kubectl -n "$NAMESPACE" wait --for=condition=ready pod -l app=data-db --timeout=120s
echo "   ✅ All components ready"
echo ""

echo "✅ Data flow restarted successfully!"
echo ""
echo "⏳ Data will publish in ~75 seconds (waiting for subscribers to connect)..."
echo "📊 Then data will flow for a few minutes (varies by scenario)."
echo ""
echo "Available scenarios:"
echo "  sample1  →  B200 GPU with no power capping (default)"
echo "  sample2  →  B200 with static power limit"
echo "  sample3  →  B200 with dynamic power management"
echo "  sample4  →  B200 with partial dynamic capping"
echo ""
echo "Switch scenarios:"
echo "  ./scripts/restart-data-flow.sh sample2"
echo "  ./scripts/restart-data-flow.sh sample3-b200-dynamic-powercap  # or use full name"

