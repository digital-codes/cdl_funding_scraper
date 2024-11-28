# Funding Crawler

## Hinweis

Diese readme.md wurde mit Hilfe einer generativen KI auf Grundlage des Codes erstellt. Sie ist möglicherweise noch nicht vollständig bzw. nicht vollständig korrekt.

## Überblick

Das `Funding Crawler`-Projekt ist ein Python-basiertes Web-Crawling-Tool, das entwickelt wurde, um Förderprogramme von der Website der Förderdatenbank das BMWK zu extrahieren. Das Tool sammelt systematisch Links zu den Förderprogrammen von den Suchergebnisseiten, ruft die verlinkten Seiten ab und extrahiert die relevanten Inhalte wie Titel, Beschreibungen und detaillierte Informationen zu den Förderprogrammen. Die Ergebnisse werden in einer strukturierten JSON-Datei gespeichert.

## Projektstruktur

Das Projekt ist in verschiedene Module und Verzeichnisse unterteilt, um die Wartbarkeit und Erweiterbarkeit zu gewährleisten:

```plaintext
funding_crawler/
│
├── config/
│   ├── __init__.py             # Kennzeichnet das config-Verzeichnis als Python-Paket
│   └── settings.py             # Zentrale Konfigurationsdatei
│
├── data/
│   ├── cache/                  # Hier wird der sqlite-basierte Cache für die geladenen HTML-Seiten angelegt
│   └── output.json             # Ausgabedatei im JSON-Format
│
├── logs/
│   └── crawler.log             # Log-Datei für das Crawling
│
├── src/
│   ├── __init__.py             # Initialisiert das src-Verzeichnis als Python-Paket
│   ├── crawler.py              # Haupt-Crawling-Logik
│   ├── parser.py               # Funktionen zum Parsen und Extrahieren von Daten aus HTML
│   ├── cache.py                # Funktionen zur Cache-Verwaltung
│   ├── utils.py                # Hilfsfunktionen (z.B. für Logging, Exception-Handling)
│   └── exporter.py             # Funktionen zum Exportieren der Daten ins JSON-Format
│
└── main.py                     # Hauptskript zum Starten des Crawlings
```

## Installationsanweisungen

1. **Klonen des Repositories:**

   ```bash
   git clone https://github.com/awodigital/funding_crawler.git
   cd funding_crawler
   ```

2. **Installiere uv:**
  
  Folge [diesen](https://docs.astral.sh/uv/getting-started/installation/) Anweisungen

3. **Installiere python requiremens**

    ```bash
   uv sync
   ```
4. **Installiere pre-commit**

    ```bash
   uv run pre-commit install
   ```
 

## Funktionsweise

Das Tool arbeitet in zwei Hauptphasen:

### Phase 1: Extrahieren der Links von den Seiten mit den Suchergebnissen

- Das Skript beginnt mit dem Abrufen der ersten Suchergebnisseite, die in der Konfiguration angegeben ist.
- Es iteriert durch die Seiten der Suchergebnisse und sammelt alle Links, die zu den detaillierten Förderprogrammen führen.
- Wenn eine Seite aus dem Cache geladen wird, entfällt die Wartezeit. Ansonsten wartet das Skript 30 Sekunden zwischen den Seitenabrufen, wie in der `robots.txt` vorgeschrieben.

### Phase 2: Abrufen und Analysieren der verlinkten Seiten

- Nach dem Sammeln der Links zu den Förderprogrammen ruft das Skript die verlinkten Seiten ab und analysiert sie.
- Es extrahiert den Titel (`h1`-Tag), den Inhalt (`div` mit Klasse `content`) und andere relevante Informationen, die in einer Tabelle dargestellt werden.
- Die extrahierten Daten werden in einer strukturierten JSON-Datei gespeichert.

## Wichtige Module

- **`crawler.py`:** Implementiert die Hauptlogik für beide Phasen des Crawlings.
- **`parser.py`:** Enthält die Funktionen zum Parsen der HTML-Seiten und zum Extrahieren der benötigten Daten.
- **`cache.py`:** Verwalten des HTTP-Caches, um unnötige Anfragen zu vermeiden.
- **`exporter.py`:** Speichern der gesammelten Daten im JSON-Format.
- **`settings.py`:** Zentrale Konfiguration des Projekts.

## Ausführung des Crawlers

Um das Tool auszuführen, verwenden Sie einfach den folgenden Befehl:

```bash
python main.py
```

Das Skript startet das Crawling gemäß den in der Konfigurationsdatei festgelegten Parametern und speichert die extrahierten Daten in der Datei `data/output.json`.

## Logging

Das Tool verwendet ein einfaches Logging-System, um Informationen über den Fortschritt des Crawlings aufzuzeichnen. Die Logs werden in der Datei `logs/crawler.log` gespeichert. Es wird protokolliert, welche Seiten abgerufen wurden, ob sie aus dem Cache geladen wurden und ob Fehler aufgetreten sind.

## Anpassungen

Die Struktur des Projekts ermöglicht es, das Tool leicht zu erweitern oder anzupassen:

- **Weitere Seitenanalyse:** Neue Analysefunktionen können in `parser.py` hinzugefügt werden.
- **Andere Exportformate:** Die `exporter.py` kann erweitert werden, um die Daten in anderen Formaten wie CSV oder SQL zu speichern.
- **Erweiterte Konfiguration:** Zusätzliche Konfigurationsparameter können in `settings.py` definiert werden.

## Lizenz

MÜSSTE GGF. BEI VERÖFFENTLICHUNG NOCH GEKLÄRT WERDEN

## Kontakt

Bei Fragen oder Vorschlägen können Sie gerne ein Issue im GitHub-Repository eröffnen oder direkt den Autor kontaktieren.
