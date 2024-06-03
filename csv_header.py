import pathlib
import csv

dir = '/home/kilian/Seafile/RELAXO/MESSUNGEN/Impedanzspektroskopie/September2023'

for path in pathlib.Path(dir).rglob('*.csv'):
    p = str(path).split('/')
    if "TEMP" in p[-2]:
        print(f"Processing file: {path}")  # Debug-Ausgabe

        with open(path) as csvfile:
            # Feste Variablen
            messgeraet_id = 1
            # Aus Dateiname
            stab_id = p[-1][4:5]
            dauer = 'unknown'
            if "OPEN" in p[-3]:
                s = p[-1].split('_')
                dauer = s[-1]
                
            # Aus Datei
            row = csvfile.readline().split(';')
            print(f"First row: {row}")  # Debug-Ausgabe
            timestamp = row[0]
            temp = row[1]
            volt = p[-2].split('_')[2]
            
            # Read some file
            spamreader = csv.reader(csvfile, delimiter=';')
            headers = next(spamreader, None)
            if headers:
                print(f"Headers: {headers}")  # Debug-Ausgabe
                d = {}
                
                # Breakpoint nach dem Lesen der Header-Zeile
                break
            else:
                print("No headers found, or the file is empty.")  # Debug-Ausgabe
