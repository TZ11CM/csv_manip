from pathlib import Path

from function import CsvManip

file_input_1 = Path("data") / "vyroba.csv"
file_input_2 = Path("data") / "meteo.csv"
file_input_3 = Path("data") / "prodpred.csv"

file_output = Path("results") / "prodpred.csv"

CsvManip.add_row_counter(file_input_3, file_output)


