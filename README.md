# Web Crawler & ABC Parser System

Een multi-process web crawler en ABC muzieknotatie parser systeem gebouwd in Python met SQLite als backend. Het systeem is ontworpen om muziekwebsites te crawlen, ABC-bestanden te downloaden en muzikale data (inclusief MIDI pitches) te extraheren.

## Componenten

### 1. URL Dispatcher (`url_dispatcher.py`)
- Beheert de centrale queue en coördineert werkzaamheden.
- Deelt URLs uit aan **Fetchers** (voor downloaden) en **Parsers** (voor verwerken).
- Handhaaft "politeness" regels (cooldown per host).
- Luistert op poort 8888 voor socket-verbindingen.

### 2. URL Fetcher (`url_fetcher.py`)
- Downloadt documenten via de dispatcher.
- Extraheert hyperlinks en voegt ze toe aan de queue.
- Respecteert `robots.txt` en filtert op MIME-types.
- Meerdere instanties kunnen parallel draaien.

### 3. URL Parser (`url_parser.py`)
- Verwerkt gedownloade documenten met de status 'fetched'.
- Gebruikt `abc_parser.py` om ABC-muzieknotatie te vinden.
- Extraheert metadata (Titel, Componist, Sleutel, etc.) en **MIDI Pitches** (via `music21`).
- Slaat resultaten op in de `tunebooks` en `tunes` tabellen.
- Meerdere instanties kunnen parallel draaien.

### 4. URL Purger (`url_purger.py`)
- Onderhoudsproces dat periodiek de database opschoont.
- Verwijdert URLs met geweigerde extensies of hosts met DNS-fouten.

### 5. Flask Web Interface (`app.py`)
- Dashboard voor procesbeheer (Start/Stop Dispatcher, Purger, Fetchers, Parsers).
- Real-time statistieken (Totaal URLs, Geparsede documenten, Gevonden tunes).
- Configuratie van MIME-types en geweigerde extensies.
- URL-queue monitoring en beheer.

## Architectuur & Workflow

Het systeem volgt een drie-fasen pipeline:
1. **Crawl**: De Dispatcher stuurt nieuwe URLs naar Fetchers.
2. **Fetch**: Fetchers downloaden de content en slaan deze op als BLOB. De status wordt 'fetched'.
3. **Parse**: De Dispatcher stuurt 'fetched' URLs naar Parsers. Parsers extraheren muziekdata en zetten de status op 'parsed'.

## Database Schema (Belangrijkste tabellen)

### `urls` tabel
- `status`: '', 'dispatched', 'fetched', 'parsing', 'parsed', 'error'.
- `document`: De ruwe gedownloade content (BLOB).
- `has_abc`: Boolean die aangeeft of er muziekdata is gevonden.
- `host`: Hostnaam voor scheduling en cooldowns.

### `tunes` tabel
- Bevat alle geparsede ABC-metadata (T, C, K, M, L, etc.).
- `tune_body`: De ruwe ABC-muziektekst.
- `pitches`: Komma-gescheiden MIDI-getallen van de noten.

## Installatie & Gebruik

1. **Requirements**:
   ```bash
   pip install -r requirements.txt
   ```
   *Opmerking: `music21` is vereist voor pitch-extractie.*

2. **Initialisatie**:
   ```bash
   python database.py
   ```

3. **Dashboard Starten**:
   ```bash
   python app.py
   ```
   Ga naar `http://localhost:5500` om het systeem te beheren.

## Beheer via Makefile
- `make start`: Start alle basisprocessen via scripts.
- `make stop`: Stopt alle draaiende processen.
- `make status`: Toont de status van de poorten en processen.
