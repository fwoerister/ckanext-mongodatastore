#!/bin/bash

set -e

python testcases/ftc1_1_publish_resource.py
python testcases/ftc2_1_insert_record.py
python testcases/ftc2_2_modify_record.py
python testcases/ftc2_3_delete_record.py
python testcases/ftc3_1_query_by_value.py
python testcases/ftc3_2_range_query.py
python testcases/ftc3_3_fulltext_query.py
python testcases/ftc3_4_sorted_query.py
python testcases/ftc4_1_pid_for_query_by_value.py
python testcases/ftc4_2_pid_for_range_query.py
python testcases/ftc4_3_pid_for_fulltext_query.py
python testcases/ftc4_4_pid_for_sorted_query.py
python testcases/ftc5_1_experiment_fetch_data_via_rest.py
python testcases/ftc5_2_experiment_fetch_data_via_cli.py

exit 0