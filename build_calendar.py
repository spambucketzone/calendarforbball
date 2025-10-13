import hashlib, re, sys, pytz, yaml
from datetime import datetime, timedelta
from dateutil import parser as dtparser
import requests
from bs4 import BeautifulSoup
from icalendar import Calendar, Event, vText

# ---------- config helpers ----------

def load_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def sha_uid(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest() + "@auto-fixtures"

# ---------- FCBQ parser (tailored to basquetcatala.cat) ----------

def parse_fcbq_schedule(url: str, tzname: str, default_minutes: int):
    """
    Parses pages like:
      https://www.basquetcatala.cat/partits/calendari_equip_global/176/81055
    and returns a list of games with:
      game_id, home, away, start, end, venue, address, url
    """
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    tz = pytz.timezone(tzname)
    games = []

    # Strategy A: try to walk the table if present (header that contains 'Data' ... 'Camp de joc')
    table = None
    for t in soup.find_all(["table", "div"]):
        txt = t.get_text(" ", strip=True)
        if ("Data" in txt and "Hora" in txt and "Camp de joc" in txt) or ("Calendari" in txt and "Camp de joc" in txt):
            table = t
            break

    # Extract linear text blocks under the schedule area,
    # then split by date lines (dd/mm/yyyy). This is resilient even if markup changes.
    # Each "block" typically looks like:
    #   04/10/2025
    #   15:30
    #   CE INFANT JESUS A  LIMA-HORTA BARCELONA  3A...  PAVELLO INFANT JESUS
    #   TRAVESSERA DE GRACIA 57-63
    container_text = (table.get_text("\n", strip=True) if table else soup.get_text("\n", strip=True))

    # Keep only the part after the schedule header if present
    m_head = re.search(r"Calendari\s+Global\s+Equip.*?\n", container_text, flags=re.I|re.S)
    if m_head:
        container_text = container_text[m_head.end():]

    # Split on date lines
    date_pat = re.compile(r"(^|\n)(\d{2}/\d{2}/\d{4})(?=\n)", flags=re.M)
    parts = date_pat.split(container_text)
    # parts is like [before, (sep), date, after, (sep), date, after, ...]
    # We iterate over date, after pairs
    for i in range(2, len(parts), 3):
        date_str = parts[i]
        after = parts[i+1] if i+1 < len(parts) else ""
        # First line after date should be time
        lines = [ln for ln in after.split("\n") if ln.strip()]
        if not lines:
            continue
        time_line = lines[0].strip()
        # Time like 15:45
        if not re.match(r"^\d{1,2}:\d{2}$", time_line):
            # Sometimes there are icons or empty lines; try to find the first time-looking line
            tmatch = next((ln for ln in lines if re.match(r"^\d{1,2}:\d{2}$", ln.strip())), None)
            if not tmatch:
                continue
            time_line = tmatch

        # Combine date + time → start
        try:
            start_local = dtparser.parse(f"{date_str} {time_line}", dayfirst=True)
        except Exception:
            continue
        if start_local.tzinfo is None:
            start_local = tz.localize(start_local)
        end_local = start_local + timedelta(minutes=default_minutes)

        # Next line(s) usually contain: HOME  AWAY  ...  VENUE_NAME
        # We'll take the first non-time line that contains two team names (there are two consecutive "links" in HTML).
        # Fallback: use the entire next non-empty line.
        # We will also check the team anchor tags around here to be safer.
        home = away = None
        venue_name = ""
        address_line = ""

        # Try to use anchors near the time: find two nearest <a> texts around the time node
        # If not reliable in text form, fallback to regex from the line with both teams.
        # Since we have only text here, do a heuristic:
        # Find the first line after the time that has at least two "  " (double space) or "  " separation.
        candidate_line = None
        for ln in lines[1:]:
            s = ln.strip()
            # Stop if we hit another date (end of block)
            if re.match(r"^\d{2}/\d{2}/\d{4}$", s):
                break
            if any(k in s for k in ["Categoria", "Camp de joc"]):
                continue
            candidate_line = s
            break

        if candidate_line:
            # The structure tends to be: HOME  AWAY  CATEGORY  VENUE
            # Split by two or more spaces to avoid splitting inside names with hyphens.
            chunks = re.split(r"\s{2,}", candidate_line)
            # Fallback: if still one chunk, split once by '  ' or single spaces around '  '
            if len(chunks) < 2:
                chunks = re.split(r"\s{1,}", candidate_line)
            # Heuristic: first two chunks are teams; the last chunk (or last tokens) include venue name
            if len(chunks) >= 2:
                home = chunks[0].strip()
                away = chunks[1].strip()
                if len(chunks) >= 3:
                    venue_name = chunks[-1].strip()

        # Address is typically the next line after the line with venue name
        # Example: "TRAVESSERA DE GRACIA 57-63"
        for ln in lines[2:]:
            s = ln.strip()
            if re.match(r"^\d{2}/\d{2}/\d{4}$", s) or re.match(r"^\d{1,2}:\d{2}$", s):
                break
            # pick the first reasonable address-looking line (contains digits or comma)
            if any(ch in s for ch in [",", "0","1","2","3","4","5","6","7","8","9"]):
                address_line = s
                break

        # Build a stable-ish ID
        gid = f"{date_str}|{time_line}|{home or ''}|{away or ''}|{venue_name or ''}"

        # Compose a human-friendly venue string
        venue_full = venue_name
        if address_line and address_line not in venue_full:
            venue_full = f"{venue_full} — {address_line}" if venue_full else address_line

        # Minimal sanity: need teams
        if not (home and away):
            # Sometimes the line packs teams with single spaces; last resort: scan anchors globally near this date block
            pass

        games.append({
            "game_id": gid,
            "home": (home or "").strip(),
            "away": (away or "").strip(),
            "start": start_local,
            "end": end_local,
            "venue": (venue_name or "").strip(),
            "location": venue_full.strip(),
            "url": url,
        })

    # Filter out entries with missing teams just in case
    games = [g for g in games if g["home"] and g["away"]]
    return games

# ---------- ICS builder ----------

def build_ics(games, cfg, outfile="schedule.ics"):
    cal = Calendar()
    cal.add('prodid', '-//Auto Fixtures (FCBQ)//EN')
    cal.add('version', '2.0')
    cal.add('name', cfg["calendar"]["name"])
    cal.add('X-WR-CALNAME', cfg["calendar"]["name"])
    cal.add('X-WR-TIMEZONE', cfg["calendar"]["timezone"])

    tpl = cfg["calendar"]["title_template"]
    for g in games:
        ev = Event()
        title = tpl.format(home=g["home"], away=g["away"], venue=g.get("venue",""))
        ev.add('summary', title)
        ev.add('dtstart', g["start"])
        ev.add('dtend', g["end"])
        if g.get("location"):
            ev.add('location', vText(g["location"]))
        desc_lines = []
        if g.get("url"): desc_lines.append(g["url"])
        ev.add('description', "\n".join(desc_lines))
        ev.add('uid', sha_uid(g["game_id"]))
        if g.get("url"):
            ev.add('url', g["url"])
        cal.add_component(ev)

    with open(outfile, "wb") as f:
        f.write(cal.to_ical())

# ---------- main ----------

def main():
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    cfg = load_config(cfg_path)
    url = cfg["source"]["url"]
    tzname = cfg["calendar"]["timezone"]
    default_minutes = int(cfg["calendar"]["default_duration_minutes"])

    # Domain-specific parser (robust against markup changes)
    games = parse_fcbq_schedule(url, tzname, default_minutes)

    # Optional filter (not needed for this page)
    inc = (cfg.get("filters") or {}).get("include_team", "").strip().lower()
    if inc:
        games = [g for g in games if inc in g["home"].lower() or inc in g["away"].lower()]

    build_ics(games, cfg, outfile="schedule.ics")
    print(f"Built schedule.ics with {len(games)} events")

if __name__ == "__main__":
    main()
