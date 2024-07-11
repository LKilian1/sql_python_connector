import pandas as pd
import pathlib

# CSV Header modifizieren
def process_csv_files():
    dir = '/home/kilian/Seafile/RELAXO/MESSUNGEN/Impedanzspektroskopie/September2023'

    for path in pathlib.Path(dir).rglob('*.csv'):
        p = str(path).split('/')
        if 'TEMP' in p[-2]:
            print(f"Processing folder: {p[-2]}")
            with open(path, 'r') as csvfile:
                first_line = csvfile.readline().strip()
            
            # 'timestamp', 'temperatur', 'spannung' extrahieren -> string manipulation
            timestamp, temperatur, u = first_line.split(';')[:3]

            timestamp = timestamp.replace('-', '').replace(' ', '').replace(':', '')[:12]
            initial_timestamp = int(timestamp)

            stab_id = int(p[-1].split('_')[0].removeprefix('Stab'))

            temperatur = float(temperatur)
            u = float(u)

            # CSV Datei einlesen und erste Zeile überspringen
            df = pd.read_csv(path, skiprows=1, delimiter=';')
            
            # Debug-Ausgabe der neuen Spaltennamen
            #print(f"Neue Spaltennamen: {df.columns}")
            
            # Spalten aus der Zeile 1 an den Tabellenheader hängen
            timestamps = [initial_timestamp + i * 10 for i in range(len(df))]
            df.insert(0, 'timestamp', timestamps)
            df.insert(1, 'temperatur', temperatur)
            df.insert(2, 'u', u)
            #print(df.columns)

            # Speichern der modifizierten Tabellenheaders
            output_path = f"/home/kilian/Documents/Python_Project/CSV/{p[-2]}_stab_{stab_id}.csv"
            df.to_csv(output_path, index=False)

# Hauptprogramm ausführen
if __name__ == "__main__":
    process_csv_files()
