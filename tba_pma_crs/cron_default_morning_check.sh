#!/bin/bash

if [ -z "$1" ]; then
    echo "Use this format: $0 <BusinessDate>"
    exit 1
fi

mkdir -p /home/teamsupport2/archive/
mkdir -p /home/teamsupport2/logs/

BUSINESSDATE=$1
#YESTERDAY=$(date -d "yesterday" +"%Y%m%d")
#YESTERDAY="20240812"
ARR_SYS=('tba' 'pma' 'crs')
REPORT_FILE="/home/teamsupport2/logs/morning_check_report_${BUSINESSDATE}.log"

BANKSIM_FILE_NAME_PATTERN="*.log"
INPUT_FILE_NAME_PATTERN="*${BUSINESSDATE}*.csv"
OUTPUT_FILE_NAME_PATTERN="*${BUSINESSDATE}*.csv"
XLS_FILE_NAME_PATTERN="*.xls"

BANKSIM_TYPE_FILE="logs"
INPUT_TYPE_FILE="input"
OUTPUT_TYPE_FILE="output"

#BANKSIM_FILE_EXT="log"
#INPUT_FILE_EXT="csv"
#OUTPUT_FILE_EXT="csv"

declare -A ARR_SYS_BANKSIM_FILES
declare -A ARR_SYS_INPUT_FILES
declare -A ARR_SYS_OUTPUT_FILES

TBA_BANKSIM_FILE_PATTERNS="eod_extract_loan_trades_:log,eod_extract_repo_trades_:log,monitor_and_load_client_trades_:log"
PMA_BANKSIM_FILE_PATTERNS="eod_extract_loan_trades_:log,eod_extract_repo_trades_:log,load_eod_trades_:log,load_market_data_:log,load_referential_data_:log"
CRS_BANKSIM_FILE_PATTERNS="load_market_data_:log,load_referential_data_:log,load_trades_:log,risk_computation_:log,risk_dataset_generation_:log"

PMA_INPUT_FILE_PATTERNS="Client_PTF_:csv,Clients_:csv,FX_:csv,eod_loan_trades_:csv,eod_repo_trades_:csv,stock_data_:csv"
CRS_INPUT_FILE_PATTERNS="Client_PTF_:csv,Clients_rating_:csv,MasterContractProductData_:csv,backoffice_repo_:csv,credit_limit_data_:csv,stock_data_:csv"

TBA_OUTPUT_FILE_PATTERNS="eod_loan_trades_:csv,eod_repo_trades_:csv"
PMA_OUTPUT_FILE_PATTERNS="backoffice_loans_:csv,backoffice_repo_:csv,collat_data_:csv"
CRS_OUTPUT_FILE_PATTERNS="risk_dataset:xls"

ARR_SYS_BANKSIM_FILES['tba']=$TBA_BANKSIM_FILE_PATTERNS
ARR_SYS_BANKSIM_FILES['pma']=$PMA_BANKSIM_FILE_PATTERNS
ARR_SYS_BANKSIM_FILES['crs']=$CRS_BANKSIM_FILE_PATTERNS

ARR_SYS_INPUT_FILES['pma']=$PMA_INPUT_FILE_PATTERNS
ARR_SYS_INPUT_FILES['crs']=$CRS_INPUT_FILE_PATTERNS

ARR_SYS_OUTPUT_FILES['tba']=$TBA_OUTPUT_FILE_PATTERNS
ARR_SYS_OUTPUT_FILES['pma']=$PMA_OUTPUT_FILE_PATTERNS
ARR_SYS_OUTPUT_FILES['crs']=$CRS_OUTPUT_FILE_PATTERNS

declare -A patterns_dir
declare -A arr_received

echo "Report File ${BUSINESSDATE}" > $REPORT_FILE

initialize_sys_info() {
    local sys=$1

    echo -e '\n' >> $REPORT_FILE
    echo "$sys $BUSINESSDATE" >> $REPORT_FILE
    echo "=====================================================" >> $REPORT_FILE
}

check_amount_files() {
    local dir_banksim=$1
    local dir_input=$2
    local dir_output=$3

    check_banksimlogs_files=$(ls -l ${dir_banksim}*log 2>/dev/null | wc -l)
    check_banksimlogs_load_files=$(ls -l ${dir_banksim}load*log 2>/dev/null | wc -l)
    check_banksimlogs_extract_files=$(ls -l ${dir_banksim}*extract*log 2>/dev/null | wc -l)
    check_input_files=$(ls -l /${dir_input}*${BUSINESSDATE}*.csv 2>/dev/null | wc -l)
    check_output_files=$(ls -l ${dir_output}*${BUSINESSDATE}*.csv 2>/dev/null | wc -l)

    echo "# of BanksImLogs log files: ${check_banksimlogs_files}" >> $REPORT_FILE
    echo "# of BanksImLogs load files: ${check_banksimlogs_load_files}" >> $REPORT_FILE
    echo "# of BanksImLogs extract files: ${check_banksimlogs_extract_files}" >> $REPORT_FILE
    echo "# of Input files: ${check_input_files}" >> $REPORT_FILE
    echo "# of Output files: ${check_output_files}" >> $REPORT_FILE
}

calculate_received() {
    local pattern_dir=$1

    countWheader=$(wc -l ${pattern_dir}.csv 2>/dev/null | awk 'END {print $1}');
    countHeaders=$(ls -l ${pattern_dir}.csv 2>/dev/null | wc -l);

    echo $(( countWheader - countHeaders ))
}

print_errors() {
    local input_dir=$1

    errors=$(grep -i 'error' ${input_dir}*log)
    error_count=$(( (echo $errors | wc -l) ))
    echo "# of errors in logs: ${error_count}" >> $REPORT_FILE

    if [[ -n "$errors" ]]; then
        echo -e "\nErrors" >> $REPORT_FILE
        echo "-------------" >> $REPORT_FILE
        echo "$errors" | while IFS= read -r line; do
            echo "$line" >> $REPORT_FILE
        done
        echo -e '\n' >> $REPORT_FILE
    fi
}

zip_files() {
    local sys=$1
    local input_dir=$2
    local file_name_pattern=$3
    local type_file=$4
    output_dir="/home/teamsupport2/archive/${sys}_${type_file}_${BUSINESSDATE}.tar.gz"

    find "${input_dir}" -type f \( -name "${file_name_pattern}" -o -name "${XLS_FILE_NAME_PATTERN}" \) -exec tar -czvf ${output_dir} {} + > /dev/null 2>&1

    if [ $? -eq 0 ]; then
        echo "${type_file^} archive created successfully" >> $REPORT_FILE
    else
        echo "Failed to create ${type_file} archive" >> $REPORT_FILE
    fi
}

check_missing_files() {
    local patterns=$1
    local directory=$2
    local date_value=$3
    missing_files=""

    IFS=',' read -r -a arr_patterns <<< "$patterns"
    for file_name_pattern in "${arr_patterns[@]}"; do
        file_name="${file_name_pattern%:*}"
        file_ext="${file_name_pattern##*:}"
        pattern=$( [ "${file_name}" != "risk_dataset" ] && echo "${directory}${file_name}${local_date}*.${file_ext}" \
            || echo "${directory}${file_name}.${file_ext}")
        if [ -z "$(ls ${pattern} 2>/dev/null)" ]; then
            missing_files+="${pattern} "
        fi
    done
    echo "$missing_files"
}

check_mandatory_files() {
    local sys=$1
    local dir=$2
    #local file_ext=$3
    local date=$3
    local arr_sys=$4
    local missing_files=""

    if [ -n "${arr_sys[$sys]}" ]; then
        missing_files+=$(check_missing_files "${arr_sys[$sys]}" $dir $date)
    fi

    echo "$missing_files"
}

check_new_missables() {
    local sys=$1
    local banksim_dir=$2
    local output_dir=$3
    local arr_sys=$4
    local missing_files=""
    local results=$(grep -r "Data successfully written to \|Data written to CSV file " "${banksim_dir}" | awk -F "Data successfully written to |Data written to CSV file " '{print $2}')
    IFS=',' read -r -a arr_patterns <<< "$arr_sys"

    while IFS= read -r line; do
        match_found=false
        filename=$(basename "$line")
        whole_filename="$filename"
        filename="${filename%.*}"
        for file_name_pattern in "${arr_patterns[@]}"; do
            file_name_pattern="${file_name_pattern%:*}"
            if [[ "$filename" == "${file_name_pattern}"* ]]; then
                match_found=true
                break
            fi
        done

        if [[ $match_found == false ]]; then
            if [ ! -e "${output_dir}${whole_filename}" ]; then
                missing_files+="${output_dir}${whole_filename} "
            fi
        fi
    done <<< "$results"

    echo "$missing_files"
}

print_missing_files_info() {
    local missing_files=$1

    if [[ -z "$missing_files" ]]; then
        echo "No missing files" >> $REPORT_FILE
    else
        echo "Missing files:" >> $REPORT_FILE
        for missing_file in $missing_files; do
            echo "${missing_file}" >> $REPORT_FILE
        done
    fi
}

assign_received() {
    for key in "${!patterns_dir[@]}"; do
        arr_received[$key]=$(calculate_received "${patterns_dir[$key]}")
    done
}

for sys in ${ARR_SYS[@]}; do
    dir_banksim="/home/azureuser/blobmount/banksimlogs/${BUSINESSDATE}/${sys}/"
    dir_input="/home/azureuser/blobmount/${sys}/data/input/"
    dir_output="/home/azureuser/blobmount/${sys}/data/output/"

    missing_files=""

    initialize_sys_info $sys

    check_amount_files $dir_banksim $dir_input $dir_output

    case $sys in
        tba)
            patterns_dir["total"]="/home/azureuser/blobmount/${sys}/data/input/*trades_${BUSINESSDATE}*"
            patterns_dir["loan"]="/home/azureuser/blobmount/${sys}/data/input/*loantrades_${BUSINESSDATE}*"
            patterns_dir["repo"]="/home/azureuser/blobmount/${sys}/data/input/*repotrades_${BUSINESSDATE}*"

            patterns_dir["tba_eod_loan"]="/home/azureuser/blobmount/${sys}/data/output/eod_loan_trades_${BUSINESSDATE}"
            patterns_dir["tba_eod_repo"]="/home/azureuser/blobmount/${sys}/data/output/eod_repo_trades_${BUSINESSDATE}"
            ;;

        pma)
            patterns_dir["total"]="/home/azureuser/blobmount/${sys}/data/input/eod_*_trades_*${BUSINESSDATE}*"
            patterns_dir["loan"]="/home/azureuser/blobmount/${sys}/data/input/eod_loan_trades_*${BUSINESSDATE}*"
            patterns_dir["repo"]="/home/azureuser/blobmount/${sys}/data/input/eod_repo_trades_*${BUSINESSDATE}*"

            pma_fx_rates=$(awk -F, 'NR > 1 {print "Rate from", $1, "to", $2, "is", $3}' /home/azureuser/blobmount/${sys}/data/input/FX_${BUSINESSDATE}*.csv);

            patterns_dir["pma_client_ptf"]="/home/azureuser/blobmount/${sys}/data/input/Client_PTF_${BUSINESSDATE}"
            patterns_dir["pma_clients"]="/home/azureuser/blobmount/${sys}/data/input/Clients_${BUSINESSDATE}"
            patterns_dir["pma_stock"]="/home/azureuser/blobmount/${sys}/data/input/stock_data_${BUSINESSDATE}"

            patterns_dir["pma_backoffice_loan"]="/home/azureuser/blobmount/${sys}/data/output/backoffice_loans_${BUSINESSDATE}"
            patterns_dir["pma_backoffice_repo"]="/home/azureuser/blobmount/${sys}/data/output/backoffice_repo_${BUSINESSDATE}"
            patterns_dir["pma_collat"]="/home/azureuser/blobmount/${sys}/data/output/collat_data_${BUSINESSDATE}"
            ;;
        crs)
            patterns_dir["total"]="/home/azureuser/blobmount/${sys}/data/input/*backoffice_*${BUSINESSDATE}*"
            patterns_dir["loan"]="/home/azureuser/blobmount/${sys}/data/input/*backoffice_loan*${BUSINESSDATE}*"
            patterns_dir["repo"]="/home/azureuser/blobmount/${sys}/data/input/*backoffice_repo*${BUSINESSDATE}*"

            crs_fx_rates=$(awk -F, 'NR > 1 {print "Rate from", $1, "to", $2, "is", $3}' /home/azureuser/blobmount/${sys}/data/input/FX_${BUSINESSDATE}*.csv);

            patterns_dir["crs_client_ptf"]="/home/azureuser/blobmount/${sys}/data/input/Client_PTF_${BUSINESSDATE}"
            patterns_dir["crs_clients"]="/home/azureuser/blobmount/${sys}/data/input/Clients_${BUSINESSDATE}"
            patterns_dir["crs_clients_rating"]="/home/azureuser/blobmount/${sys}/data/input/Clients_rating_${BUSINESSDATE}"
            patterns_dir["crs_master_product"]="/home/azureuser/blobmount/${sys}/data/input/MasterContractProductData_${BUSINESSDATE}"
            patterns_dir["crs_collat"]="/home/azureuser/blobmount/${sys}/data/input/collat_data_${BUSINESSDATE}"
            patterns_dir["crs_credit_limit"]="/home/azureuser/blobmount/${sys}/data/input/credit_limit_data_${BUSINESSDATE}"
            patterns_dir["crs_master_contract"]="/home/azureuser/blobmount/${sys}/data/input/master_contract_${BUSINESSDATE}"
            patterns_dir["crs_stock"]="/home/azureuser/blobmount/${sys}/data/input/stock_data_${BUSINESSDATE}"
            ;;
    esac
    arr_received["total"]=$(calculate_received "${patterns_dir["total"]}")
    arr_received["loan"]=$(calculate_received "${patterns_dir["loan"]}")
    arr_received["repo"]=$(calculate_received "${patterns_dir["repo"]}")

    echo "Total Trades received in Input: ${arr_received["total"]}" >> $REPORT_FILE
    echo "Total Loan received in Input: ${arr_received["loan"]}" >> $REPORT_FILE
    echo "Total Repo received in Input: ${arr_received["repo"]}" >> $REPORT_FILE

    print_errors $dir_banksim

    zip_files $sys $dir_banksim $BANKSIM_FILE_NAME_PATTERN $BANKSIM_TYPE_FILE
    zip_files $sys $dir_input $INPUT_FILE_NAME_PATTERN $INPUT_TYPE_FILE
    zip_files $sys $dir_output $OUTPUT_FILE_NAME_PATTERN $OUTPUT_TYPE_FILE

    missing_files+=$(check_mandatory_files $sys $dir_banksim "" "${ARR_SYS_BANKSIM_FILES[$sys]}")
    missing_files+=$(check_mandatory_files $sys $dir_input $BUSINESSDATE "${ARR_SYS_INPUT_FILES[$sys]}")
    missing_files+=$(check_mandatory_files $sys $dir_output $BUSINESSDATE "${ARR_SYS_OUTPUT_FILES[$sys]}")

    missing_files+=$(check_new_missables $sys $dir_banksim $dir_output "${ARR_SYS_OUTPUT_FILES[$sys]}")

    echo -e '\n' >> $REPORT_FILE

    print_missing_files_info "$missing_files"

done

SQL_COMMANDS_FILE="/home/teamsupport2/sql_commands_current_morning_check.txt"
formatted_date=$(echo "$BUSINESSDATE" | sed 's/\(....\)\(..\)\(..\)/\1-\2-\3/')
echo "$formatted_date" > $SQL_COMMANDS_FILE

echo "SELECT COUNT(*) AS 'TBA Trades' FROM TradeBooking.Trades WHERE TradeDate='$formatted_date';" >> $SQL_COMMANDS_FILE
echo "SELECT COUNT(*) AS 'TBA LoanTrades' FROM TradeBooking.LoanTrades WHERE TradeDate='$formatted_date';" >> $SQL_COMMANDS_FILE
echo "SELECT COUNT(*) AS 'TBA RepoTrades' FROM TradeBooking.RepoTrades WHERE TradeDate='$formatted_date';" >> $SQL_COMMANDS_FILE

echo "SELECT COUNT(*) AS 'PMA Trades' FROM PoseManagement.Trades WHERE TradeDate='$formatted_date';" >> $SQL_COMMANDS_FILE
echo "SELECT COUNT(*) AS 'PMA LoanTrades' FROM PoseManagement.LoanTrades WHERE TradeDate='$formatted_date';" >> $SQL_COMMANDS_FILE
echo "SELECT COUNT(*) AS 'PMA RepoTrades' FROM PoseManagement.RepoTrades WHERE TradeDate='$formatted_date';" >> $SQL_COMMANDS_FILE

echo "SELECT COUNT(*) AS 'CRA Loan' FROM creditriskdb.BackOffice_Loan WHERE Timestamp='$formatted_date';" >> $SQL_COMMANDS_FILE
echo "SELECT COUNT(*) AS 'CRA Repo' FROM creditriskdb.BackOffice_Repo WHERE Timestamp='$formatted_date';" >> $SQL_COMMANDS_FILE

ONE_SQL_TABLE_COMMANDS_FILE="/home/teamsupport2/one_sql_table_commands_current_morning_check.txt"
echo "$formatted_date" > $ONE_SQL_TABLE_COMMANDS_FILE

echo "SELECT
    (SELECT COUNT(*) FROM TradeBooking.Trades WHERE TradeDate='$formatted_date') AS 'TBA_Trades',
    (SELECT COUNT(*) FROM TradeBooking.LoanTrades WHERE TradeDate='$formatted_date') AS 'TBA_LoanTrades',
    (SELECT COUNT(*) FROM TradeBooking.RepoTrades WHERE TradeDate='$formatted_date') AS 'TBA_RepoTrades',
    (SELECT COUNT(*) FROM PoseManagement.Trades WHERE TradeDate='$formatted_date') AS 'PMA_Trades',
    (SELECT COUNT(*) FROM PoseManagement.LoanTrades WHERE TradeDate='$formatted_date') AS 'PMA_LoanTrades',
    (SELECT COUNT(*) FROM PoseManagement.RepoTrades WHERE TradeDate='$formatted_date') AS 'PMA_RepoTrades',
    (SELECT COUNT(*) FROM creditriskdb.BackOffice_Loan WHERE Timestamp='$formatted_date') AS 'CRS_LoanTrades',
    (SELECT COUNT(*) FROM creditriskdb.BackOffice_Repo WHERE Timestamp='$formatted_date') AS 'CRS_RepoTrades';" >> $ONE_SQL_TABLE_COMMANDS_FILE

echo -e '\n' >> $REPORT_FILE
assign_received

cat <<EOF >> $REPORT_FILE
Additional optional information
=====================================================
Total TBA's EOD Loans received in Output: ${arr_received["tba_eod_loan"]}
Total TBA's EOD repo received in Output: ${arr_received["tba_eod_repo"]}

PMA FX:
${pma_fx_rates}
Total PMA's Client PTF received in Input: ${arr_received["pma_client_ptf"]}
Total PMA's Clients received in Input: ${arr_received["pma_clients"]}
Total PMA's Stocks received in Input: ${arr_received["pma_stock"]}
Total PMA's backoffice loans received in Output: ${arr_received["pma_backoffice_loan"]}
Total PMA's backoffice repo received in Output: ${arr_received["pma_backoffice_repo"]}
Total PMA's collat received in Output: ${arr_received["pma_collat"]}

CRS FX:
${crs_fx_rates}
Total CRS's Client PTF received in Input: ${arr_received["crs_client_ptf"]}
Total CRS's Clients received in Input: ${arr_received["crs_clients"]}
Total CRS's Clients Ratings received in Input: ${arr_received["crs_clients_rating"]}
Total CRS's Master Contract Product received in Input: ${arr_received["crs_master_product"]}
Total CRS's collat received in Input: ${arr_received["crs_collat"]}
Total CRS's credit limit received in Input: ${arr_received["crs_credit_limit"]}
Total CRS's Master Contract received in Input: ${arr_received["crs_master_contract"]}
Total CRS's Stocks received in Input: ${arr_received["crs_stock"]}
EOF


cp $REPORT_FILE /home/teamsupport2/current_morning_check_report.log
