import pandas as pd
import pathlib
import csv

def table_to_tables():
    dir = '/home/kilian/Documents/Python_Project/CSV'

    for path in pathlib.Path(dir).rglob('*.csv'):
        p = str(path).split('/')
        if 'TEMP' in p[-1]:
            with open(path, 'r') as csvfile:
                # Lade CSV Datei
                df = pd.read_csv(path)
                print(df.columns[3])
                exit(0)

                # Definiere zusätzliche Spalten
                additional_columns_table1 = {
                    'mess_id_t' : 1,
                    'stab_id' : int(p[-1].split('_')[4].removesuffix('.csv')),
                    'messgeraet_id': 3,
                    'messreihe_id' : 1
                }

                additional_columns_table2 = {
                    'mess_id_z' : 6,
                    'stab_id' : int(p[-1].split('_')[4].removesuffix('.csv')),
                    'messgeraet_id': 1,
                    'messreihe_id' : 1
                }

                additional_columns_table3 ={
                    'mess_id_i_u' : 1,
                    'stab_id' : int(p[-1].split('_')[4].removesuffix('.csv')),
                    'messgeraet_id': 2,
                    'messreihe_id' : 1
                }

                


                # Definieren der Spaltenzuordnung für die Tabellen
                # Tabelle 1 == messung_t
                columns_table1 = [df.columns[0], df.columns[1]]
            
                
                # Initialisiere Tabelle 1 mit den vorhandenen Daten aus den CSV Spalten
                table1 = df[columns_table1].copy()
                # Hinzufügen der zusätzlichen Spalten
                for col, value in additional_columns_table1.items():
                    table1[col] = value


                # Speichern der modifizierten Tabelle
                output_path = '/home/kilian/Documents/Python_Project/tables/first_try.csv'
                table1.to_csv(output_path, index=False) 




# Hauptprogramm ausführen
if __name__ == "__main__":
    table_to_tables()