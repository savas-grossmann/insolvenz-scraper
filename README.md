# Insolvenz-Tracker
Ein Python-Scraper für [insolvenzbekanntmachungen.de](https://neu.insolvenzbekanntmachungen.de), der eine Kundenliste mit veröffentlichten Insolvenzbekanntmachungen abgleicht.

## Übersicht

Dieses Projekt automatisiert die Überwachung von Insolvenzbekanntmachungen in Deutschland.  
Es sammelt neue Insolvenzeinträge aus dem offiziellen Register, normalisiert Firmennamen und vergleicht diese mit einer Kundenliste (aus einer Datenbank).  
Mögliche Treffer werden durch exaktes oder Fuzzy-Matching erkannt und können in der Datenbank gespeichert werden.

## Funktionen

- Ruft Insolvenzbekanntmachungen von insolvenzbekanntmachungen.de ab  
- Normalisiert Firmennamen für konsistentes Matching  
- Vergleicht Bekanntmachungen mit Kundendaten aus einer Datenbank  
- Unterstützt exakte und unscharfe Übereinstimmungen (über RapidFuzz)  
- Speichert Ergebnisse in Datenbank und/oder CSV/JSON  

## Voraussetzungen

- Python 3.10+  
- Datenbank mit Kundendaten  
- Umgebungsvariablen für den Datenbankzugriff und SQL-Abfragen (siehe `.env`-Datei)

Abhängigkeiten (installierbar mit `pip install -r requirements.txt`):

- requests  
- beautifulsoup4  
- pandas  
- python-dotenv  
- connector-python  
- rapidfuzz  

## Einrichtung

1. Repository klonen:

```bash
git clone https://github.com/savas-grossmann/insolvenz-scraper.git
cd insolvency-tracker
```

2. Virtuelle Umgebung erstellen und Abhängigkeiten installieren:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Eine `.env`-Datei mit den Datenbankeinstellungen anlegen:

```env
DB_HOST=
DB_PORT=
DB_USER=
DB_PASSWORD=
DB_DATABASE=
CLIENT_QUERY=
INSERT_QUERY=
LOG_QUERY=
```

## Verwendung

Scraper mit Standardeinstellungen ausführen (heutige Bekanntmachungen):

```bash
python insolvenztracker.py
```

Scraper für vergangene Tage ausführen (z. B. 7 Tage rückwirkend):

```bash
python insolvenztracker.py --days-back 7
```

Debug-Modus aktivieren (speichert Rohdaten zur Analyse):

```bash
python insolvenztracker.py --debug
```

## Ausgabe

- Treffer & Logs werden in der konfigurierten Datenbank gespeichert.
- Zusätzlich können Ergebnisse auch als CSV- oder JSON-Dateien exportiert werden.  

## Projektstruktur

```
├── insolvenztracker.py      # Hauptskript
├── requirements.txt         # Abhängigkeiten
├── README.md
```

## Hinweis
Bitte berücksichtigen Sie die Nutzungsbedingungen von [insolvenzbekanntmachungen.de](https://neu.insolvenzbekanntmachungen.de).