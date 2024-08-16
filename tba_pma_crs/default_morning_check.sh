#!/bin/bash

if [ -z "$1" ]; then
    echo "Use this format: $0 <BusinessDate>"
    exit 1
fi

mkdir -p /home/teamsupport2/archive/

BUSINESSDATE=$1
#YESTERDAY=$(date -d "yesterday" +"%Y%m%d")
#YESTERDAY="20240812"
ARR_SYS=('tba' 'pma' 'crs')

#DIR_BANKSIM="/home/azureuser/blobmount/banksimlogs/${BUSINESSDATE}/"

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
#declare -A FILE_PATTERNS_TO_CHECK_SERVER_DATE

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

#FILE_PATTERNS_TO_CHECK_SERVER_DATE['tba']=$TBA_BANKSIM_FILE_PATTERNS
#FILE_PATTERNS_TO_CHECK_SERVER_DATE['pma']=$PMA_BANKSIM_FILE_PATTERNS
#FILE_PATTERNS_TO_CHECK_SERVER_DATE['crs']=$CRS_BANKSIM_FILE_PATTERNS

declare -A patterns_dir
declare -A arr_received

initialize_sys_info() {
    local sys=$1

    echo -e '\n'
    echo "$sys $BUSINESSDATE"
    echo "====================================================="
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

    echo "# of BanksImLogs log files: ${check_banksimlogs_files}"
    echo "# of BanksImLogs load files: ${check_banksimlogs_load_files}"
    echo "# of BanksImLogs extract files: ${check_banksimlogs_extract_files}"
    echo "# of Input files: ${check_input_files}"
    echo "# of Output files: ${check_output_files}"
}

calculate_received() {
    local sys=$1
    local pattern_dir=$2

    if [[ $sys == 'crs' ]]; then
        formatted_date=$(echo $BUSINESSDATE | sed 's/\(....\)\(..\)\(..\)/\1-\2-\3/')
        countWheader=$(awk -F, -v date="$formatted_date" '$NF == date' ${pattern_dir}.csv | wc -l 2>/dev/null)
        countHeaders=0
    else
        countWheader=$(wc -l ${pattern_dir}.csv 2>/dev/null | awk 'END {print $1}');
        countHeaders=$(ls -l ${pattern_dir}.csv 2>/dev/null | wc -l);
    fi

    echo $(( countWheader - countHeaders ))
}

print_errors() {
    local input_dir=$1
    errors=$(grep -Ei 'error|critical' ${input_dir}*log)
    error_count=0
    if [[ -z "$errors" ]]; then
        error_count=0
    else
        error_count=$(echo "$errors"  | wc -l)
    fi

    echo "# of errors in logs: ${error_count}"

    if [[ -n "$errors" ]]; then
        echo -e "\nErrors"
        echo "-------------"
        echo "$errors" | while IFS= read -r line; do
            echo "$line"
        done
        echo -e '\n'
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
        echo "${type_file^} archive created successfully"
    else
        echo "Failed to create ${type_file} archive"
    fi
}

check_missing_files() {
    local patterns=$1
    local directory=$2
    local ext=$3
    local local_date=$4
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
        echo "No missing files"
    else
        echo "Missing files:"
        for missing_file in $missing_files; do
            echo "${missing_file}"
        done
    fi
}

assign_received() {
    for key in "${!patterns_dir[@]}"; do
        arr_received[$key]=$(calculate_received "" "${patterns_dir[$key]}")
    done
}

#WIP Non-functional Not needed as of 20240814
update_latest_server_run_date() {
    local arr_sys=$1
    local dir=$2
    local max_date=0
    local curr_date=0

    for sys in ${arr_sys[@]}; do
        curr_dir="${dir}${sys}/"
        for file_pattern in ${FILE_PATTERNS_TO_CHECK_SERVER_DATE[@]}; do
            match_file=$(ls "${file_pattern}*" 2>/dev/null)
            if [[ -n "$match_file" ]]; then
                filename=$(basename "${match_file}")
                date=0
            fi
        done
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

            patterns_dir["tba_trades"]="/home/azureuser/blobmount/${sys}/data/input/*_trades_${BUSINESSDATE}*"
            arr_received["tba_trades"]=$(calculate_received $sys "${patterns_dir["tba_trades"]}")
            echo "Total Trades received in Input: ${arr_received["tba_trades"]}"

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
    arr_received["total"]=$(calculate_received $sys "${patterns_dir["total"]}")
    arr_received["loan"]=$(calculate_received $sys "${patterns_dir["loan"]}")
    arr_received["repo"]=$(calculate_received $sys "${patterns_dir["repo"]}")

    echo "Total All Trades received in Input: ${arr_received["total"]}"
    echo "Total Loan received in Input: ${arr_received["loan"]}"
    echo "Total Repo received in Input: ${arr_received["repo"]}"

    print_errors $dir_banksim

    zip_files $sys $dir_banksim $BANKSIM_FILE_NAME_PATTERN $BANKSIM_TYPE_FILE
    zip_files $sys $dir_input $INPUT_FILE_NAME_PATTERN $INPUT_TYPE_FILE
    zip_files $sys $dir_output $OUTPUT_FILE_NAME_PATTERN $OUTPUT_TYPE_FILE

    missing_files+=$(check_mandatory_files $sys $dir_banksim "" "${ARR_SYS_BANKSIM_FILES[$sys]}")
    missing_files+=$(check_mandatory_files $sys $dir_input $BUSINESSDATE "${ARR_SYS_INPUT_FILES[$sys]}")
    missing_files+=$(check_mandatory_files $sys $dir_output $BUSINESSDATE "${ARR_SYS_OUTPUT_FILES[$sys]}")

    missing_files+=$(check_new_missables $sys $dir_banksim $dir_output "${ARR_SYS_OUTPUT_FILES[$sys]}")

    echo -e '\n'

    print_missing_files_info "$missing_files"

done
echo -e '\n'
assign_received

echo "Additional optional information"
echo "====================================================="
echo "Total TBA's EOD Loans received in Output: ${arr_received["tba_eod_loan"]}"
echo "Total TBA's EOD repo received in Output: ${arr_received["tba_eod_repo"]}"
echo -e '\n'
echo "PMA FX: ${pma_fx_rates}"
echo "Total PMA's Client PTF received in Input: ${arr_received["pma_client_ptf"]}"
echo "Total PMA's Clients received in Input: ${arr_received["pma_clients"]}"
echo "Total PMA's Stocks received in Input: ${arr_received["pma_stock"]}"
echo "Total PMA's backoffice loans received in Output: ${arr_received["pma_backoffice_loan"]}"
echo "Total PMA's backoffice repo received in Output: ${arr_received["pma_backoffice_repo"]}"
echo "Total PMA's collat received in Output: ${arr_received["pma_collat"]}"
echo -e '\n'
echo "CRS FX: ${crs_fx_rates}"
echo "Total CRS's Client PTF received in Input: ${arr_received["crs_client_ptf"]}"
echo "Total CRS's Clients received in Input: ${arr_received["crs_clients"]}"
echo "Total CRS's Clients Ratings received in Input: ${arr_received["crs_clients_rating"]}"
echo "Total CRS's Master Contract Product received in Input: ${arr_received["crs_master_product"]}"
echo "Total CRS's collat received in Input: ${arr_received["crs_collat"]}"
echo "Total CRS's credit limit received in Input: ${arr_received["crs_credit_limit"]}"
echo "Total CRS's Master Contract received in Input: ${arr_received["crs_master_contract"]}"
echo "Total CRS's Stocks received in Input: ${arr_received["crs_stock"]}"
