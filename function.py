import csv
import pandas as pd
from datetime import datetime, timedelta

class CsvManip:
    '''Třída zabývající se úpravou csv souborů s časovými řadami
    '''
    def add_missing_ts(input_file, output_file):
        '''Doplnění chybějícího timestepu do časové řady
        Hodnoty v doplněném timestepu jsou totožné z předchozím známým timestepem
        Vstupy:
            input_file:         Vstupní soubor
            output_file:        Výstupní soubor
        '''   
        existujici_data = {}
        with open(input_file, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)
            num_columns = len(header) - 1
            column_values = [[] for _ in range(num_columns)]
            
            for row in reader:
                date = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
                value = [float(value) for value in row[1:]]
                existujici_data[date] = value
                
                for i in range(num_columns):
                    column_values[i].append(value[i])

        min_date = min(existujici_data.keys())
        max_date = max(existujici_data.keys())
        
        previous_values = [None] * num_columns

        current_date = min_date
        while current_date <= max_date:
            if current_date in existujici_data:
                previous_values = existujici_data[current_date]
            else:
                existujici_data[current_date] = previous_values[:] 
            current_date += timedelta(minutes=1)  

        serazena_data = sorted(existujici_data.items(), key=lambda x: x[0])

        with open(output_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            for date, value in serazena_data:
                writer.writerow([date.strftime('%Y-%m-%d %H:%M:%S')] + value)
        return
    
    def add_row_counter(input_file, output_file):
        '''Přidání sloupce představujícího počítadlo řádků
        '''
        with open(input_file, 'r') as file:
            reader = csv.reader(file)
            rows = list(reader)

        rows[0].insert(0, "Row Counter")
        for i, row in enumerate(rows[1:], start=0):
            row.insert(0, str(i))
        
        with open(output_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)
        return

    def aggregate_data(input_file, output_file):
        '''Upraví velikost časových oken
        '''
        # Otevřít vstupní soubor CSV a přečíst data
        with open(input_file, 'r') as file:
            reader = csv.reader(file)
            rows = list(reader)

        aggregated_data = {}

        # Projít každý řádek a agregovat hodnoty podle čtvrthodinových okének
        for row in rows:
            if len(row) > 1:
                try:
                    timestamp = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
                    value = float(row[1])

                    # Zaokrouhlení časové známky na nejbližší čtvrthodinu dolů
                    rounded_timestamp = timestamp.replace(minute=0, second=0, microsecond=0)

                    # Přidání hodnoty do agregovaných dat pro dané časové okno
                    if rounded_timestamp in aggregated_data:
                        aggregated_data[rounded_timestamp] += value
                    else:
                        aggregated_data[rounded_timestamp] = value
                except (ValueError, IndexError):
                    pass

        # Vytvořit seznam řádků pro výstupní soubor
        output_rows = [rows[0]]

        # Projít agregovaná data a přidat je do seznamu řádků
        for timestamp, value in sorted(aggregated_data.items()):
            output_rows.append([timestamp.strftime('%Y-%m-%d %H:%M:%S'), value])

        # Uložit výstupní data do souboru CSV
        with open(output_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(output_rows)
    
    def merge_two_csvs(input_file_1, input_file_2, output_file):
        '''Spojení dvou csv souborů do jednoho
        Vstupy:
            input_file_1:       Vstupní soubor 1
            input_file_2:       Vstupní soubor 2
            output_file:        Výstupní soubor
        '''
        if not output_file.exists():
            output_file.touch()

        input_file_1 = CsvManip.remove_rows_with_na(input_file_1, input_file_1)
        input_file_1 = CsvManip.adjust_times_in_csv(input_file_1, input_file_1)
        input_file_2 = CsvManip.remove_rows_with_na(input_file_2, input_file_2)
        input_file_2 = CsvManip.adjust_times_in_csv(input_file_2, input_file_2)

        data1 = {}
        with input_file_1.open('r') as file1:
            reader1 = csv.reader(file1)
            header1 = next(reader1)  
            for row1 in reader1:
                date1 = datetime.strptime(row1[0], '%Y-%m-%d %H:%M:%S')
                value1 = [float(value) for value in row1[1:]]
                data1[date1] = value1

        spojena_data = []
        with input_file_2.open('r') as file2:
            reader2 = csv.reader(file2)
            header2 = next(reader2)
            for row2 in reader2:
                date2 = datetime.strptime(row2[0], '%Y-%m-%d %H:%M:%S')
                value2 = [float(value) for value in row2[1:]]
                if date2 in data1:
                    spojena_data.append([date2, data1[date2], value2])

        with output_file.open('w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['datetimes'] + header1[1:] + header2[1:]) 
            for row in spojena_data:
                writer.writerow([row[0].strftime('%Y-%m-%d %H:%M:%S')] + row[1] + row[2])
        return
    
    ###
    
    def adjust_times_in_csv(input_file, output_file):
        # Otevřít vstupní soubor CSV a přečíst data
        with open(input_file, 'r') as file:
            reader = csv.reader(file)
            rows = list(reader)

        # Projít každý řádek a upravit časovou značku
        for row in rows:
            if len(row) > 0:
                try:
                    date = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
                    date = date.replace(second=0)  # Nastavit sekundy na 0
                    row[0] = date.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    pass

        # Uložit upravená data do výstupního souboru CSV
        with open(output_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)
        return output_file

    def remove_rows_with_na(input_file, output_file):
        # Načtení vstupního CSV souboru do pandas DataFrame
        data = pd.read_csv(input_file)
        
        # Odstranění řádků obsahujících NA hodnoty
        data = data.dropna()
        
        # Uložení upraveného DataFrame do výstupního CSV souboru
        data.to_csv(output_file, index=False)
        return output_file
