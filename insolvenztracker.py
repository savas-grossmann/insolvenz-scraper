import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import csv
import logging
import argparse
import pandas as pd
from dotenv import load_dotenv
import mysql.connector
import re
from rapidfuzz import fuzz, process
import json

# setup logging and empty old logs
with open('insolvency_scraper.log', 'w'):
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('insolvency_scraper.log'),
        logging.StreamHandler()
    ]
)

def normalize(name):
    if pd.isna(name) or name == '':
        return ''
    name = name.lower()
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    standard_rechtsformen = {
        'gesellschaft mit beschränkter haftung': 'gmbh',
        'gmbh & co. kg': 'gmbh co kg',
        'gmbh & co kg': 'gmbh co kg',
        'gmbh&co.kg': 'gmbh co kg',
        'gmbh&co kg': 'gmbh co kg',
        'aktiengesellschaft': 'ag',
        'kommanditgesellschaft': 'kg',
        'offene handelsgesellschaft': 'ohg',
        'gesellschaft bürgerlichen rechts': 'gbr',
        'eingetragener verein': 'ev',
        'e.v.': 'ev',
        'unternehmergesellschaft': 'ug',
        'ug (haftungsbeschränkt)': 'ug',
        'ltd.': 'ltd',
        'limited': 'ltd',
        'haftungsbeschränkt': 'ltd',
    }
    for k, v in standard_rechtsformen.items():
        name = name.replace(k, v)
    name = name.strip()
    return name


def hybrid_fuzz(a: str, b: str, **kwargs) -> float:
    # calculate and average both scores
    set_score = fuzz.token_set_ratio(a, b)
    sort_score = fuzz.token_sort_ratio(a, b)
    combined = (set_score + sort_score) / 2

    # penalty if lengths different
    len_ratio = min(len(a), len(b)) / max(len(a), len(b))
    penalized = (combined * 0.85) + (combined * len_ratio * 0.15)

    return penalized


class Matcher:
    """
    Überprüft die übergebenen Insolvenzbekanntmachungen nach Überschneidungen mit den Kundendaten.
    """
    def __init__(self, clients = None, insolvencies = None):
        self.insolvencies = insolvencies
        self.clients = clients
        self.matches = []

    def flatten_insolvency_data(data):
        flattened = []
        for entry in data:
            # Unpack the known structure: first 6 items + 1 dict
            (
                type_,
                confidence,
                client_id,
                name,
                full_name,
                insolvency_company,
                insolvency_entry
            ) = entry

            # Extract values from the nested dict
            flat_entry = (
                type_,
                confidence,
                client_id,
                name,
                full_name,
                insolvency_company,
                insolvency_entry.get('Veröffentlichungsdatum'),
                insolvency_entry.get('Aktenzeichen'),
                insolvency_entry.get('Gericht'),
                insolvency_entry.get('Firmenname'),
                insolvency_entry.get('Sitz'),
                insolvency_entry.get('Register'),
                insolvency_entry.get('scraped_at')
            )
            flattened.append(flat_entry)
        return flattened

    def find_matches(self):
        """Prüft auf Übereinstimmung"""
        if self.clients is None or len(self.clients) == 0:
            logging.error("Kann kein Matching durchführen, keine Kundendaten gefunden.")
        elif self.insolvencies is None or len(self.insolvencies) == 0:
            logging.error("Kann kein Matching durchführen, keine Insolvenzdaten gefunden.")
        logging.info("Überprüfe Kunden auf Insolvenzbekanntmachungen...")
        num_exact_matches = 0
        num_soft_matches = 0
        for insolvency in self.insolvencies:
            insolvency_company = insolvency["Firmenname"]
            if not insolvency_company:
                continue
            insolvency_company = normalize(insolvency_company)

            # Exact Matching
            exact_matches = self.clients[
                self.clients["full_name"] == insolvency_company
            ]
            if len(exact_matches) > 0:
                for exact_match in exact_matches.iterrows():
                    self.matches.append((
                        "exact",
                        100,
                        exact_match[0],
                        exact_match[1].iloc[1],
                        exact_match[1].iloc[2],
                        insolvency_company,
                        json.dumps(insolvency, ensure_ascii=False)
                    ))
                num_exact_matches += 1
                continue

            # Soft Matching with fuzzing
            clients_names = self.clients["full_name"].tolist()
            # noinspection PyTypeChecker
            fuzzy_matches = process.extract(query=insolvency_company,
                                            choices=clients_names,
                                            scorer=hybrid_fuzz,
                                            score_cutoff=80
                                            )
            if len(fuzzy_matches) > 0:
                for fuzzy_match in fuzzy_matches:
                    confidence_level = fuzzy_match[1]
                    client_id = fuzzy_match[2]
                    self.matches.append((
                        "soft",
                        confidence_level,
                        client_id,
                        self.clients.iloc[client_id]["name"],
                        self.clients.iloc[client_id]["full_name"],
                        insolvency_company,
                        json.dumps(insolvency, ensure_ascii=False)
                    ))
                num_soft_matches += 1
        logging.info(f"Insolvenzbekanntmachungen durchsucht. Exakte Treffer: {num_exact_matches}, Soft Treffer: {num_soft_matches}")
        return self.matches

class Connector:
    """
    Connector für MySQL Datenbank, holt Kundendaten und speichert ggf. Insolvenzbekanntmachungen.
    """
    def __init__(self):
        try:
            load_dotenv()
            logging.info("Versuche Datenbank zu erreichen...")
            self.db = mysql.connector.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=os.getenv('DB_DATABASE')
            )
            self.client_query = os.getenv('CLIENT_QUERY')
            self.insert_query = os.getenv('INSERT_QUERY')
            self.log_query = os.getenv('LOG_QUERY')
            logging.info("Datenbank Verbindung erfolgreich eingerichtet")
        except mysql.connector.Error as err:
            logging.error("Fehler beim erreichen der Datenbank: ", err)

    def fetch_clients(self):
        """Sammelt Kundendaten"""
        if self.db and self.db.is_connected():
            logging.info("Versuche Kundendaten zu laden aus Datenbank...")
            with self.db.cursor() as cursor:
                cursor.execute(self.client_query)
                rows = cursor.fetchall()
                if len(rows) == 0:
                    logging.error("Keine Kundendaten gefunden.")
                else:
                    logging.info(f"Kundendaten erfolgreich geladen ({len(rows)} Einträge)")
                result = pd.DataFrame(rows, columns=["id", "name", "full_name"])
                result["full_name"] = result["full_name"].apply(normalize)
                return result

    def insert_insolvencies(self, matches=None):
        """Fügt alle Matches in die Datenbank ein"""
        if not self.db and not self.db.is_connected():
            logging.error("Datenbank-Verbindung ist nicht bereit, Ergebnisse werden lokal gespeichert.")
            return False
        elif not matches:
            logging.info("Keine Treffer gefunden, nichts gespeichert.")
            return False
        with self.db.cursor() as cursor:
            cursor.executemany(self.insert_query, matches)
            self.db.commit()
            logging.info("Daten erfolgreich in Datenbank gespeichert")

    def update_log(self):
        """Ladet neue Logdatei hoch"""
        with open("insolvency_scraper.log", "r") as f:
            logs = f.readlines()
            logs = [(line.strip(),) for line in logs]
            self.db.cursor().executemany(self.log_query, logs)
            self.db.commit()

    def close(self):
        """Terminiert DB Connection"""
        self.db.close()
        logging.info("Datenbank Verbindung erfolgreich terminiert")

class InsolvencyScraper:
    """
    Simpler Scraper für insolvenzbekanntmachungen.de
    Liest ebenfalls eine Liste von Kunden ein und überprüft auf Überschneidungen.
    """
    def __init__(self, debug=False):
        self.session = requests.Session()
        self.base_url = "https://neu.insolvenzbekanntmachungen.de"
        self.search_url = "https://neu.insolvenzbekanntmachungen.de/ap/suche.jsf"
        self.debug = debug

        # Header
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de,en-US;q=0.7,en;q=0.3',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Priority': 'u=0, i'
        })

    def get_initial_page(self):
        """Standardseite laden, um den ViewState zu extrahieren"""
        try:
            logging.info("Lade initiale Suchseite...")
            response = self.session.get(self.search_url)
            response.raise_for_status()

            if self.debug:
                with open('initial_page.html', 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logging.info("Initiale Seite in 'initial_page.html' gespeichert")

            soup = BeautifulSoup(response.content, 'html.parser')

            # ViewState extrahieren von Jakarta Faces
            viewstate_input = soup.find('input', {'name': 'jakarta.faces.ViewState'})
            viewstate = viewstate_input['value'] if viewstate_input else None

            if viewstate:
                logging.info(f"ViewState gefunden: {viewstate[:50]}...")
            else:
                logging.error("Kein ViewState gefunden!")
                # Falls kein ViewState gefunden, versuche alternativen
                viewstate_input = soup.find('input', {'name': 'javax.faces.ViewState'})
                viewstate = viewstate_input['value'] if viewstate_input else None
                if viewstate:
                    logging.info(f"Legacy ViewState gefunden: {viewstate[:50]}...")

            return soup, viewstate, response.cookies

        except Exception as e:
            logging.error(f"Fehler beim Laden der initialen Seite: {e}")
            return None, None, None

    def search_insolvencies(self, viewstate, cookies, search_params=None):
        """Lade alle Insolvenzbekanntmachungen für gesetzten Tag"""

        # Standard wird auf heute gesetzt
        if search_params is None:
            today = datetime.now().strftime("%Y-%m-%d")
            search_params = {
                'datum_von': today,
                'datum_bis': today,
            }

        # Jakarta Parameter für Request
        post_data = {
            'frm_suche': 'frm_suche',
            'frm_suche:lsom_bundesland:lsom': '--+Alle+Bundesländer+--',
            'frm_suche:ldi_datumVon:datumHtml5': search_params['datum_von'],
            'frm_suche:ldi_datumBis:datumHtml5': search_params['datum_bis'],
            'frm_suche:lsom_wildcard:lsom': '0',
            'frm_suche:litx_firmaNachName:text': '',
            'frm_suche:litx_vorname:text': '',
            'frm_suche:litx_sitzWohnsitz:text': '',
            'frm_suche:iaz_aktenzeichen:itx_abteilung': '',
            'frm_suche:iaz_aktenzeichen:som_registerzeichen': '--',
            'frm_suche:iaz_aktenzeichen:itx_lfdNr': '',
            'frm_suche:iaz_aktenzeichen:itx_jahr': '',
            'frm_suche:lsom_gegenstand:lsom': '--+Alle+Gegenstände+innerhalb+des+Verfahrens+--',
            'frm_suche:ireg_registereintrag:som_registergericht': '--',
            'frm_suche:ireg_registereintrag:som_registerart': '--',
            'frm_suche:ireg_registereintrag:itx_registernummer': '',
            'frm_suche:cbt_suchen': 'Suchen',
            'jakarta.faces.ViewState': viewstate
        }

        logging.info(f"Suche für Zeitraum: {search_params['datum_von']} bis {search_params['datum_bis']}")

        if self.debug:
            logging.info(f"POST Data: {json.dumps(post_data, indent=2, ensure_ascii=False)}")

        try:
            # Referer setzen auf Standardseite
            headers = self.session.headers.copy()
            headers['Referer'] = self.search_url

            response = self.session.post(
                self.search_url,
                data=post_data,
                headers=headers
            )
            response.raise_for_status()

            if self.debug:
                with open('search_results.html', 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logging.info("Suchergebnisse in 'search_results.html' gespeichert")

            return self.parse_results(response.content)

        except Exception as e:
            logging.error(f"Fehler bei der Suche: {e}")
            return []

    def parse_results(self, html_content):
        """Extrahiere Suchergebnisse"""
        soup = BeautifulSoup(html_content, 'html.parser')
        results = []

        # Debug: Alle Tabellen und Listen in der Antwort finden
        if self.debug:
            tables = soup.find_all('table')
            logging.info(f"Gefundene Tabellen: {len(tables)}")

            for i, table in enumerate(tables):
                logging.info(f"Tabelle {i}: {table.get('class', 'keine Klasse'), table.get('id')}")

        # Selektoren für Tabelle (manchmal funktioniert tbl_ergebnis nicht, obwohl die Tabelle so heißt)
        possible_selectors = [
            'tbl_ergebnis',
            'tbody tr'
        ]

        for selector in possible_selectors:
            try:
                rows = soup.select(selector)
                if rows and len(rows) > 1:
                    # leere werden ignoriert
                    logging.info(f"Verwende Selektor: {selector} - {len(rows)} Zeilen gefunden")
                    break
            except:
                continue
        else:
            # Falls keine spezifischen gefunden, einfach alle Tabellenreihen scrapen
            rows = soup.find_all('tr')
            logging.warning(f"Fallback verwendet - {len(rows)} tr-Elemente gefunden")

        # Parsen & Speichern von Ergebnissen
        for i, row in enumerate(rows):
            if i == 0:
                continue

            try:
                cells = row.find_all(['td', 'th'])
                # skip, falls nicht genug Spalten (Richtige hat 7, aber die letzte ist irrelevant)
                if len(cells) < 6:
                    continue
                result = {
                    'Veröffentlichungsdatum': cells[0].get_text(strip=True),
                    'Aktenzeichen': cells[1].get_text(strip=True),
                    'Gericht': cells[2].get_text(strip=True),
                    'Firmenname': cells[3].get_text(strip=True),
                    'Sitz': cells[4].get_text(strip=True),
                    'Register': cells[5].get_text(strip=True),
                    'raw_data': [cell.get_text(strip=True) for cell in cells],
                    'scraped_at': datetime.now().isoformat()
                }
                if result["Firmenname"]:
                    results.append(result)

            except Exception as e:
                logging.error(f"Fehler beim Parsen von Zeile {i}: {e}")
                continue

        logging.info(f"Insgesamt {len(results)} Ergebnisse geparst")
        return results

    def save_to_csv(self, results, filename=None):
        """Speichere Ergebnisse in CSV"""
        if not filename:
            filename = f"insolvenzen_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        if not results:
            logging.info("Keine Daten zu speichern")
            return

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = results[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for result in results:
                writer.writerow(result)

        logging.info(f"Daten gespeichert in: {filename}")
        return filename

    def save_to_json(self, results, filename=None):
        """Speichere Ergebnisse in JSON"""
        if not filename:
            filename = f"insolvenzen_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        if not results:
            logging.info("Keine Daten zu speichern")
            return

        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(results, jsonfile, indent=2, ensure_ascii=False)

        logging.info(f"JSON-Daten gespeichert in: {filename}")
        return filename

    def scrape(self, days_back=0):
        """Startet Hauptfunktionen für Scraping"""
        target_date = datetime.now() - timedelta(days=days_back)
        logging.info(f"Starte Scraping für {target_date.strftime('%Y-%m-%d')}")

        # ViewState extrahieren von Startseite
        soup, viewstate, cookies = self.get_initial_page()

        if not viewstate:
            logging.error("Konnte ViewState nicht extrahieren - Abbruch")
            return

        # Suche durchführen
        search_params = {
            'datum_von': target_date.strftime('%Y-%m-%d'),
            'datum_bis': target_date.strftime('%Y-%m-%d')
        }
        results = self.search_insolvencies(viewstate, cookies, search_params)
        # Prüfen ob Clients in Insolvenzbekanntmachungen
        connector = Connector()
        clients = connector.fetch_clients()
        matcher = Matcher(clients, results)
        matches = matcher.find_matches()

        # Relevante Ergebnisse speichern
        if len(matches) > 0:
            logging.info(f"Ergebnisse ({len(matches)} Treffer) werden gespeichert...")
            connector.insert_insolvencies(matches=matches)
        connector.update_log()
        connector.close()

def setup():
    parser = argparse.ArgumentParser(description='Insolvenzbekanntmachungen Scraper')
    parser.add_argument('--debug', action='store_true', help='Debug-Modus aktivieren')
    parser.add_argument('--days-back', type=int, default=0, help='Tage rückwirkend (0 = heute)')

    args = parser.parse_args()

    # start Scraper
    scraper = InsolvencyScraper(debug=args.debug)
    scraper.scrape(days_back=args.days_back)

if __name__ == "__main__":
    setup()