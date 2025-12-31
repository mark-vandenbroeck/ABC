# Web Crawler System

Een multi-process web crawler systeem gebouwd in Python met SQLite als backend.

## Componenten

### 1. URL Dispatcher (`url_dispatcher.py`)
- Haalt URLs op uit de database in volgorde van creatie
- Levert URLs aan fetcher processen via sockets
- Beheert de communicatie tussen dispatcher en fetchers

### 2. URL Fetcher (`url_fetcher.py`)
- Downloadt URLs die door de dispatcher zijn aangeleverd
- Extraheert hyperlinks uit gedownloade documenten
- Voegt nieuwe URLs toe aan de database
- Respecteert robots.txt bestanden
- Filtert op geconfigureerde MIME types
- Meerdere fetchers kunnen parallel draaien

### 3. Flask Web Interface (`app.py`)
- Start/stop processen
- Voeg fetchers toe of verwijder ze
- Bekijk de URL queue
- Configureer MIME types

## Installatie

1. Installeer de dependencies:
```bash
pip install -r requirements.txt
```

2. Initialiseer de database:
```bash
python database.py
```

## Gebruik

1. Start de Flask web interface:
```bash
python app.py
```

2. Open je browser en ga naar `http://localhost:5000`

3. Via de web interface kun je:
   - De dispatcher starten/stoppen
   - Fetchers toevoegen/verwijderen
   - URLs toevoegen aan de queue
   - De URL queue bekijken
   - MIME types configureren

## Database Schema

### urls tabel
- `id`: Primaire sleutel
- `url`: De URL (uniek)
- `created_at`: Tijdstip van aanmaken
- `downloaded_at`: Tijdstip van download
- `size_bytes`: Grootte in bytes
- `status`: '' (nieuw), 'fetched' (gedownload), 'parsed' (verwerkt)
- `mime_type`: Het MIME type van het document
- `document`: Het gedownloade document (BLOB)

### mime_types tabel
- `id`: Primaire sleutel
- `pattern`: MIME type patroon (ondersteunt wildcards zoals `text/*`)
- `enabled`: Of het patroon actief is (1 of 0)

## Configuratie

MIME types kunnen geconfigureerd worden via de web interface. Standaard zijn de volgende types toegestaan:
- `text/html`
- `text/plain`
- `text/*`

Wildcards worden ondersteund, bijvoorbeeld:
- `text/*` - Alle text types
- `application/json` - Specifiek JSON type

## Architectuur

Het systeem gebruikt socket communicatie tussen de dispatcher en fetchers:
- Dispatcher luistert op poort 8888
- Fetchers verbinden met de dispatcher om URLs te vragen
- Fetchers sturen resultaten terug naar de dispatcher

Alle processen kunnen onafhankelijk draaien en worden beheerd via de Flask web interface.

