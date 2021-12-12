# Mock scheduled run of the `raw` ETL stage

START_TIME="2020-10-22 00:00:00"

PREV_TIME=$START_TIME
for i in $(seq 5 5 1440); do
    # Uses BSD `date` command
    END_TIME=$(date -v +$(echo $i)M -jf '%Y-%m-%d %H:%M:%S' "$START_TIME" '+%Y-%m-%d %H:%M:%S')

    # Fetch raw data
    python cli.py raw "$PREV_TIME" "$END_TIME"
    PREV_TIME=$END_TIME
done
