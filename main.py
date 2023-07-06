from pathlib import Path

from function import CsvManip

file_input_1 = Path("data") / "vyroba.csv"
file_input_2 = Path("data") / "meteo.csv"
file_input_3 = Path("data") / "prodpred.csv"

mezikrok_1 = Path("results") / "vyroba_m.csv"
mezikrok_2 = Path("results") / "meteo_m.csv"

file_output = Path("results") / "prodpred.csv"

CsvManip.aggregate_data(file_input_1, mezikrok_1)
CsvManip.add_missing_ts(file_input_2, mezikrok_2)
CsvManip.merge_two_csvs(mezikrok_1, mezikrok_2, file_output)
CsvManip.add_row_counter(file_output, file_output)


