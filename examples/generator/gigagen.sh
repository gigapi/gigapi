#!/bin/bash

# Default values
DB_NAME="mydb"
SERIES_NAME="weather"
TAGS=("location=us-midwest,season=summer" "location=us-east,season=summer" "location=us-west,season=summer")
VALUE_NAME="temperature"
MIN_VALUE=70
MAX_VALUE=100
FREQUENCY=5
API_URL="https://gigapi:7971/write"

# Backfill mode defaults
BACKFILL_MODE=false
BACKFILL_DURATION="1h"
BACKFILL_INTERVAL="1m"
BATCH_SIZE=100

# Function to show usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  -d, --db NAME           Database name (default: $DB_NAME)"
    echo "  -s, --series NAME       Series name (default: $SERIES_NAME)"
    echo "  -t, --tags TAG1,TAG2    Comma-separated tags (default: location=us-midwest,season=summer)"
    echo "  -v, --value-name NAME   Value field name (default: $VALUE_NAME)"
    echo "  -m, --min VALUE         Minimum random value (default: $MIN_VALUE)"
    echo "  -M, --max VALUE         Maximum random value (default: $MAX_VALUE)"
    echo "  -f, --frequency SEC     Frequency in seconds for real-time mode (default: $FREQUENCY)"
    echo "  -u, --url URL           API URL (default: $API_URL)"
    echo ""
    echo "Backfill Mode Options:"
    echo "  -b, --backfill          Enable backfill mode"
    echo "  -D, --duration DURATION Duration to backfill (default: $BACKFILL_DURATION)"
    echo "                          Examples: 1h, 30m, 2h30m, 1d"
    echo "  -i, --interval INTERVAL Interval between data points (default: $BACKFILL_INTERVAL)"
    echo "                          Examples: 1m, 30s, 5m"
    echo "  -B, --batch-size SIZE   Number of points to send per request (default: $BATCH_SIZE)"
    echo ""
    echo "  -h, --help              Show this help"
    echo ""
    echo "Examples:"
    echo "  Real-time mode:"
    echo "    $0 -d mydb -s cpu -v usage -m 0 -M 100 -f 10"
    echo ""
    echo "  Backfill mode:"
    echo "    $0 -b -D 2h -i 30s -d mydb -s cpu -v usage -m 0 -M 100"
    echo "    $0 --backfill --duration 1d --interval 5m --batch-size 500"
    echo "    $0 -b -D 1h -i 1m -B 1000  # Send 1000 points per request"
    exit 1
}

# Function to convert duration to seconds
duration_to_seconds() {
    local duration="$1"
    local total_seconds=0
    
    # Extract days, hours, minutes, seconds
    if [[ $duration =~ ([0-9]+)d ]]; then
        total_seconds=$((total_seconds + ${BASH_REMATCH[1]} * 86400))
    fi
    if [[ $duration =~ ([0-9]+)h ]]; then
        total_seconds=$((total_seconds + ${BASH_REMATCH[1]} * 3600))
    fi
    if [[ $duration =~ ([0-9]+)m ]]; then
        total_seconds=$((total_seconds + ${BASH_REMATCH[1]} * 60))
    fi
    if [[ $duration =~ ([0-9]+)s ]]; then
        total_seconds=$((total_seconds + ${BASH_REMATCH[1]}))
    fi
    
    # If no unit specified, assume seconds
    if [[ $duration =~ ^[0-9]+$ ]]; then
        total_seconds=$duration
    fi
    
    echo $total_seconds
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--db)
            DB_NAME="$2"
            shift 2
            ;;
        -s|--series)
            SERIES_NAME="$2"
            shift 2
            ;;
        -t|--tags)
            IFS=' ' read -ra TAGS <<< "$2"
            shift 2
            ;;
        -v|--value-name)
            VALUE_NAME="$2"
            shift 2
            ;;
        -m|--min)
            MIN_VALUE="$2"
            shift 2
            ;;
        -M|--max)
            MAX_VALUE="$2"
            shift 2
            ;;
        -f|--frequency)
            FREQUENCY="$2"
            shift 2
            ;;
        -u|--url)
            API_URL="$2"
            shift 2
            ;;
        -b|--backfill)
            BACKFILL_MODE=true
            shift
            ;;
        -D|--duration)
            BACKFILL_DURATION="$2"
            shift 2
            ;;
        -i|--interval)
            BACKFILL_INTERVAL="$2"
            shift 2
            ;;
        -B|--batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Function to generate random value
random_value() {
    echo $(( RANDOM % (MAX_VALUE - MIN_VALUE + 1) + MIN_VALUE ))
}

# Function to send batch of data
send_batch() {
    local batch_data="$1"
    local batch_count="$2"
    
    echo "Sending batch of $batch_count points..."
    
    echo "$batch_data" | curl -s -X POST "${API_URL}?db=${DB_NAME}" --data-binary @/dev/stdin
    
    if [ $? -eq 0 ]; then
        echo "✓ Batch sent successfully ($batch_count points)"
        return 0
    else
        echo "✗ Failed to send batch"
        return 1
    fi
}

# Function to send data (real-time mode)
send_data_realtime() {
    local tag_set="$1"
    local value=$(random_value)
    local line_protocol="${SERIES_NAME},${tag_set} ${VALUE_NAME}=${value}"
    
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Sending: $line_protocol"
    
    echo "$line_protocol" | curl -s -X POST "${API_URL}?db=${DB_NAME}" --data-binary @/dev/stdin
    
    if [ $? -eq 0 ]; then
        echo "✓ Data sent successfully"
    else
        echo "✗ Failed to send data"
    fi
}

# Backfill mode
run_backfill() {
    echo "Starting backfill mode..."
    echo "Duration: $BACKFILL_DURATION"
    echo "Interval: $BACKFILL_INTERVAL"
    echo "Batch size: $BATCH_SIZE"
    echo ""
    
    # Convert durations to seconds
    local duration_seconds=$(duration_to_seconds "$BACKFILL_DURATION")
    local interval_seconds=$(duration_to_seconds "$BACKFILL_INTERVAL")
    
    if [ $duration_seconds -eq 0 ] || [ $interval_seconds -eq 0 ]; then
        echo "Error: Invalid duration or interval format"
        echo "Use formats like: 1h, 30m, 2h30m, 1d"
        exit 1
    fi
    
    if [ $BATCH_SIZE -le 0 ]; then
        echo "Error: Batch size must be greater than 0"
        exit 1
    fi
    
    # Calculate timestamps
    local now_ns=$(date +%s%N)
    local start_ns=$((now_ns - (duration_seconds * 1000000000)))
    local current_ns=$start_ns
    local interval_ns=$((interval_seconds * 1000000000))
    local total_points=$(( (now_ns - start_ns) / interval_ns ))
    
    echo "Backfilling from $(date -d "@$((start_ns / 1000000000))" '+%Y-%m-%d %H:%M:%S') to $(date -d "@$((now_ns / 1000000000))" '+%Y-%m-%d %H:%M:%S')"
    echo "Total points to generate: $total_points"
    echo "Number of batches: $(( (total_points + BATCH_SIZE - 1) / BATCH_SIZE ))"
    echo ""
    
    local count=0
    local batch_count=0
    local batch_data=""
    local batch_success_count=0
    
    while [ $current_ns -le $now_ns ]; do
        # Pick a random tag set
        tag_index=$(( RANDOM % ${#TAGS[@]} ))
        selected_tag="${TAGS[$tag_index]}"
        
        # Generate data point
        local value=$(random_value)
        local line_protocol="${SERIES_NAME},${selected_tag} ${VALUE_NAME}=${value} ${current_ns}"
        
        # Add to batch
        if [ -z "$batch_data" ]; then
            batch_data="$line_protocol"
        else
            batch_data="$batch_data"$'\n'"$line_protocol"
        fi
        
        ((batch_count++))
        ((count++))
        current_ns=$((current_ns + interval_ns))
        
        # Send batch when it reaches the desired size or we've processed all points
        if [ $batch_count -eq $BATCH_SIZE ] || [ $current_ns -gt $now_ns ]; then
            if send_batch "$batch_data" "$batch_count"; then
                batch_success_count=$((batch_success_count + batch_count))
            fi
            
            # Show progress
            if [ $total_points -gt 0 ]; then
                local percentage=$(( count * 100 / total_points ))
                echo "Progress: $count/$total_points ($percentage%) - Successfully sent: $batch_success_count"
            fi
            
            # Reset batch
            batch_data=""
            batch_count=0
        fi
    done
    
    echo ""
    echo "Backfill completed!"
    echo "Total points generated: $count"
    echo "Successfully sent: $batch_success_count"
    echo "Failed: $((count - batch_success_count))"
}

# Real-time mode
run_realtime() {
    echo "Starting real-time mode..."
    echo "Database: $DB_NAME"
    echo "Series: $SERIES_NAME"
    echo "Tags: ${TAGS[*]}"
    echo "Value range: $MIN_VALUE - $MAX_VALUE"
    echo "Frequency: ${FREQUENCY}s"
    echo "Press Ctrl+C to stop"
    echo ""

    while true; do
        # Pick a random tag set
        tag_index=$(( RANDOM % ${#TAGS[@]} ))
        selected_tag="${TAGS[$tag_index]}"
        
        send_data_realtime "$selected_tag"
        
        sleep "$FREQUENCY"
    done
}

# Main execution
if [ "$BACKFILL_MODE" = true ]; then
    run_backfill
else
    run_realtime
fi
