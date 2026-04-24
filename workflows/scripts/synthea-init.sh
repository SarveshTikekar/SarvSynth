#!/bin/bash
set -e

# --- Configuration ---
NUMBER_OF_PATIENTS=${1:-150}
STATE=${2:-Massachusetts}
PROJ_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
REPO_URL="https://github.com/synthetichealth/synthea.git"
SYNTHEA_DIR="$HOME/synthea"
DATASET_DIR="$PROJ_DIR/Datasets"
LOG_FILE="$PROJ_DIR/synthea_generation.log"

# --- Function for Logging ---
log_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log_msg "🚀 Starting Synthea Data Generation for $NUMBER_OF_PATIENTS patients..."

# --- 1. Clone/Update Logic ---
if [ ! -d "$SYNTHEA_DIR" ]; then
    log_msg "🔄 Synthea not found. Cloning repository..."
    git clone "$REPO_URL" "$SYNTHEA_DIR"
    FIRST_RUN=true
else
    log_msg "✅ Synthea repo found at $SYNTHEA_DIR."
    FIRST_RUN=false
fi

# --- 2. Build & Configure (Only on First Run) ---
PROPERTIES_FILE="$SYNTHEA_DIR/src/main/resources/synthea.properties"

if [ "$FIRST_RUN" = true ]; then
    log_msg "⚙️  Configuring synthea.properties (One-time setup)..."
    
    # Enable CSV, Append Mode, and set Output Directory
    sed -i 's/^exporter.fhir.export *= *.*/exporter.fhir.export = true/' "$PROPERTIES_FILE"
    sed -i 's/^exporter.csv.export *= *.*/exporter.csv.export = true/' "$PROPERTIES_FILE"
    sed -i 's/^exporter.csv.append_mode *= *.*/exporter.csv.append_mode = true/' "$PROPERTIES_FILE"
    sed -i "s|^exporter.baseDirectory *= *.*|exporter.baseDirectory = $DATASET_DIR|" "$PROPERTIES_FILE"
    
    cd "$SYNTHEA_DIR"
    log_msg "🔨 Building Synthea (skipping tests for speed)..."
    ./gradlew build -x test
else
    log_msg "⏩ Skipping build (already built previously)."
fi

# --- 3. Execute Generation ---
mkdir -p "$DATASET_DIR"
cd "$SYNTHEA_DIR"

log_msg "🏃 Executing Synthea for $NUMBER_OF_PATIENTS patients in $STATE..."
./run_synthea -p "$NUMBER_OF_PATIENTS" "$STATE" | tee -a "$LOG_FILE"

log_msg "✅ Success! Data generated in: $DATASET_DIR"
