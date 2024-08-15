#!/bin/bash

DATE_FILE="/home/teamsupport2/scripts/date_tracker.txt"
CURR_DATE=$(cat $DATE_FILE)

bash /home/teamsupport2/scripts/cron_default_morning_check.sh $CURR_DATE

NEXT_DATE=$(date -d "${CURR_DATE} +1 day" +"%Y%m%d")

day_of_week=$(date -d "$NEXT_DATE" +%u)

if [[ "$day_of_week" -eq 6 ]]; then
    NEXT_DATE=$(date -d "${NEXT_DATE} +2 days" +"%Y%m%d")
elif [[ "$day_of_week" -eq 7 ]]; then
    NEXT_DATE=$(date -d "${NEXT_DATE} +1 day" +"%Y%m%d")
fi

echo $NEXT_DATE > $DATE_FILE
