import pandas as pd

# Pfad zur CSV-Datei
file_path = '/home/kilian/Documents/Python_Project/modified_data_2.csv'
# CSV-Datei einlesen
data = pd.read_csv(file_path)

# IDs und konstante Werte definieren
messreihe_id = 6  # Beispielwert, anpassen nach Bedarf
stab_id = 4  # Beispielwert, anpassen nach Bedarf
mess_id_t = 1
mess_id_z = 2
mess_id_i_u = 3
messgeraet_id_t = 1
messgeraet_id_z = 6
messgeraet_id_i_u = 1

# Zusätzliche konstante Spalten hinzufügen
data['stab_id'] = stab_id
data['messreihe_id'] = messreihe_id
data['deleted'] = False  # Beispielwert, anpassen nach Bedarf

# Tabelle 1 erstellen
data['mess_id_t'] = mess_id_t
data['messgeraet_id_t'] = messgeraet_id_t
tabelle1_columns = ['timestamp', 'mess_id_t', 'stab_id', 'messgeraet_id_t', 'messreihe_id', 'temperatur', 'deleted']
tabelle1 = data[tabelle1_columns]
tabelle1 = tabelle1.rename(columns={'messgeraet_id_t': 'messgeraet_id'})

# Tabelle 2 erstellen
data['mess_id_z'] = mess_id_z
data['messgeraet_id_z'] = messgeraet_id_z
tabelle2_columns = ['timestamp', 'mess_id_z', 'stab_id', 'messgeraet_id_z', 'messreihe_id', 
                    'absz', 'abszpwr', 'abszstddev', 'frequency', 'frequencypwr', 'frequencystddev', 
                    'imagz', 'imagzpwr', 'imagzstddev', 'param0', 'param0pwr', 'param0stddev', 
                    'param1', 'param1pwr', 'param1stddev', 'phasez', 'phasezpwr', 'phasezstddev', 
                    'realz', 'realzpwr', 'realzstddev', 'deleted']
tabelle2 = data[tabelle2_columns]
tabelle2 = tabelle2.rename(columns={'messgeraet_id_z': 'messgeraet_id'})

# Tabelle 3 erstellen
data['mess_id_i_u'] = mess_id_i_u
data['messgeraet_id_i_u'] = messgeraet_id_i_u
# Fehlende Spalten für Tabelle 3 hinzufügen, falls nicht vorhanden
if 'i' not in data.columns:
    data['i'] = None  # oder setzen Sie einen Standardwert
if 'u' not in data.columns:
    data['u'] = None  # oder setzen Sie einen Standardwert
tabelle3_columns = ['timestamp', 'mess_id_i_u', 'stab_id', 'messgeraet_id_i_u', 'messreihe_id', 'i', 'u', 'deleted']
tabelle3 = data[tabelle3_columns]
tabelle3 = tabelle3.rename(columns={'messgeraet_id_i_u': 'messgeraet_id'})

# Tabellen als CSV-Dateien speichern
tabelle1.to_csv('/home/kilian/Documents/Python_Project/tabelle1.csv', index=False)
tabelle2.to_csv('/home/kilian/Documents/Python_Project/tabelle2.csv', index=False)
tabelle3.to_csv('/home/kilian/Documents/Python_Project/tabelle3.csv', index=False)

# Ausgabe der Tabellenköpfe zur Kontrolle
tabelle1.head(), tabelle2.head(), tabelle3.head()
