import pandas as pd
import pathlib
import csv

# Globale Variablen für die Startwerte der IDs
mess_id_t = 1
mess_id_z = 6
mess_id_i_u = 1
messreihe_id = 6

def get_current_ids():
    global mess_id_t, mess_id_z, mess_id_i_u, messreihe_id
    # Speichere die aktuellen Werte
    current_ids = {
        'mess_id_t': mess_id_t,
        'mess_id_z': mess_id_z,
        'mess_id_i_u': mess_id_i_u,
        'messreihe_id': messreihe_id
    }
    return current_ids

def increment_ids():
    global mess_id_t, mess_id_z, mess_id_i_u, messreihe_id
    # Inkrementiere die Werte
    mess_id_t += 1
    mess_id_z += 1
    mess_id_i_u += 1
    messreihe_id += 1


def table_to_tables():
    dir = '/home/kilian/Documents/Python_Project/CSV'

    for path in pathlib.Path(dir).rglob('*.csv'):
        p = str(path).split('/')
        if 'TEMP' in p[-1]:
            with open(path, 'r') as csvfile:
                # Lade CSV Datei
                df = pd.read_csv(path, delimiter=';')

                first_line = csvfile.readline().strip()
                header = first_line.split(',')

                 # Hole die aktuellen IDs
                ids = get_current_ids()

                # Definiere zusätzliche Spalten
                additional_columns_table1 = {
                    'mess_id_t' : ids['mess_id_t'],
                    'stab_id' : int(p[-1].split('_')[4].removesuffix('.csv')),
                    'messgeraet_id': 3,
                    'messreihe_id' : ids['messreihe_id']
                }

                additional_columns_table2 = {
                    'mess_id_z' : ids['mess_id_z'],
                    'stab_id' : int(p[-1].split('_')[4].removesuffix('.csv')),
                    'messgeraet_id': 1,
                    'messreihe_id' : ids['messreihe_id']
                }

                additional_columns_table3 ={
                    'mess_id_i_u' : ids['mess_id_i_u'],
                    'stab_id' : int(p[-1].split('_')[4].removesuffix('.csv')),
                    'messgeraet_id': 2,
                    'messreihe_id' : ids['messreihe_id']
                }

                
                # Definieren der Spaltenzuordnung für die Tabellen
                # Tabelle 1 == messung_t
                columns_table1 = [header[0], header[1]]

                # Initialisiere Tabelle 1 mit den vorhandenen Daten aus den CSV Spalten
                table1 = df[columns_table1].copy()
                print(table1)

                # Hinzufügen der zusätzlichen Spalten
                for col, value in additional_columns_table1.items():
                    table1[col] = value
                
                # Spaltenreihenfolge für Tabelle 3 anpassen
                new_order_table1 = [header[0]] + list(additional_columns_table1.keys()) + [header[1]]
                table1 = table1[new_order_table1]
                print(f"Debug Tabelle 1 (messung_t): {new_order_table1}")
                print(table1.head())
                exit(0)

                # Definieren der Spaltenzuordnung für die Tabellen
                # Tabelle 2 == messung_z
                columns_table2 = ['timestamp'] + header[3:24]
        
                # Initialisiere Tabelle 2 mit den vorhandenen Daten aus den CSV Spalten
                table2 = df[columns_table2].copy()
                # Hinzufügen der zusätzlichen Spalten
                for col, value in additional_columns_table2.items():
                    table2[col] = value
                
                # Spaltenreihe  nfolge für Tabelle 2 anpassen
                new_order_table2 = ['timestamp'] + list(additional_columns_table2.keys()) + header[3:23]
                table2 = table2[new_order_table2]
                print(f"Debug Tabelle 2 (messung_z): {new_order_table2}")
                print(table2.head())
                exit(0)

                # Definieren der Spaltenzuordnung für die Tabellen
                # Tabelle 3 == messung_i_u
                columns_table3 = [df.columns[0], df.columns[2]]
        
                # Initialisiere Tabelle 3 mit den vorhandenen Daten aus den CSV Spalten
                table3 = df[columns_table3].copy()
                # Hinzufügen der zusätzlichen Spalten
                for col, value in additional_columns_table3.items():
                    table3[col] = value

                # Spaltenreihenfolge für Tabelle 3 anpassen
                new_order_table3 = [df.columns[0]] + list(additional_columns_table3.keys()) + [df.columns[2]]
                table3 = table3[new_order_table3]
                
                stab_id = int(p[-1].split('_')[4].replace('.csv', ''))
                # Speichern der modifizierten Tabelle
                output_path_table1 = f'/home/kilian/Documents/Python_Project/tables/{p[-2]}_stab_{stab_id}_t.csv'
                output_path_table2 = f'/home/kilian/Documents/Python_Project/tables/{p[-2]}_stab_{stab_id}_z.csv'
                output_path_table3 = f'/home/kilian/Documents/Python_Project/tables/{p[-2]}_stab_{stab_id}_i_u.csv'
                table1.to_csv(output_path_table1, index=False)
                table2.to_csv(output_path_table2, index=False)
                table3.to_csv(output_path_table3, index=False) 

                # Inkrementiere die IDs für die nächste CSV-Datei
                increment_ids()



# Hauptprogramm ausführen
if __name__ == "__main__":
    table_to_tables()