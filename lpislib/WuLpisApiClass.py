#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import pickle
import datetime
from typing import Any, Dict

import mechanize
from bs4 import BeautifulSoup
from lxml import html


class WuLpisApi:
    URL = "https://lpis.wu.ac.at/lpis"

    def __init__(self, username: str = "", password: str = "", args=None, sessiondir: str | None = None):
        self.username = username or ""
        self.password = password or ""
        self.matr_nr = self.username[1:] if self.username else ""
        self.args = args

        self.data: Dict[str, Any] = {}
        self.status: Dict[str, Any] = {}

        # --- mechanize browser setup ---
        self.browser = mechanize.Browser()
        self.browser.set_handle_robots(False)
        self.browser.set_handle_refresh(False)
        self.browser.set_handle_equiv(True)
        self.browser.set_handle_redirect(True)
        self.browser.set_handle_referer(True)
        self.browser.set_debug_http(False)
        self.browser.set_debug_responses(False)
        self.browser.set_debug_redirects(False)
        self.browser.addheaders = [
            (
                "User-agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/68.0.3440.106 Safari/537.36",
            ),
            ("Accept", "*/*"),
        ]

        # Optional session file location (not required for Railway)
        self.sessionfile = os.path.join(sessiondir or "sessions", self.username)

        # Login immediately (kept for compatibility with the original project)
        self.login()

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------

    def _open_and_soup(self, url: str) -> BeautifulSoup:
        """Open URL via mechanize and return BeautifulSoup(document)."""
        res = self.browser.open(url)
        return BeautifulSoup(res.read(), "html.parser")

    @staticmethod
    def _clean_text(s: str) -> str:
        """Collapse whitespace and trim."""
        return re.sub(r"\s+", " ", (s or "").strip())

    # ---------------------------------------------------------------------
    # Login flow
    # ---------------------------------------------------------------------

    def login(self) -> Dict[str, Any]:
        """
        Perform LPIS login and derive the post-login base URL we use to navigate.
        Returns a simple structure (kept for compatibility).
        """
        self.data = {}

        # Open the login page
        r = self.browser.open(self.URL)

        # Select the login form (fallback to first form if LPIS changes the name)
        try:
            self.browser.select_form("login")
        except mechanize.FormNotFoundError:
            forms = list(self.browser.forms())
            if not forms:
                raise RuntimeError("Login form not found on LPIS landing page.")
            self.browser.form = forms[0]

        # Read BYTES (not str) and remove HTML comments as BYTES to keep encodings intact
        raw = r.read()  # bytes
        cleaned = re.sub(rb"<!--.*?-->", b"", raw, flags=re.S)

        # Parse with lxml from BYTES (avoid 'Unicode strings with encoding declaration' errors)
        tree = html.fromstring(cleaned)

        # Find the username/password input names using access keys (LPIS quirk)
        usernames = list(set(tree.xpath("//input[@accesskey='u']/@name")))
        passwords = list(set(tree.xpath("//input[@accesskey='p']/@name")))
        if not usernames or not passwords:
            raise RuntimeError("Could not locate LPIS username/password fields on login page.")

        input_username = usernames[0]
        input_password = passwords[0]

        # Fill and submit
        self.browser[input_username] = self.username
        self.browser[input_password] = self.password
        r2 = self.browser.submit()

        # After login, LPIS redirects to a path we use as "base"
        url = r2.geturl()
        if "/" not in url:
            raise RuntimeError("Unexpected LPIS redirect URL after login.")
        self.URL_scraped = url[: url.rindex("/") + 1]

        # Status info
        self.status["last_logged_in"] = datetime.datetime.now()
        self.data = self.URL_scraped  # for compat with older code
        return self.data

    # ---------------------------------------------------------------------
    # Navigation to the overview that contains the study-plan form/table
    # ---------------------------------------------------------------------

    def ensure_overview(self) -> None:
        """
        Ensure we are on the study-plan overview page that contains either:
          - a form named 'ea_stupl', or
          - any form that has a control named 'ASPP'
        Tries multiple fallbacks by following likely navigation links.
        """
        # 1) Open the post-login base page
        soup = self._open_and_soup(self.URL_scraped)

        # Quick check: is the form already here?
        if soup.find("form", {"name": "ea_stupl"}) or soup.find("select", {"name": "ASPP"}):
            return

        # 2) Try clicking obvious links by text/href heuristics
        candidates: list[str] = []
        for a in soup.find_all("a"):
            text = (a.get_text() or "").strip().lower()
            href = (a.get("href") or "").strip()
            if not href:
                continue
            # Heuristics tuned for LPIS navigation terms
            if (
                ("stupl" in href.lower())
                or ("lehrveranstaltungs" in text)
                or ("anmeldung" in text)
                or ("studien" in text)
            ):
                candidates.append(href)

        for href in candidates:
            try:
                url = href if href.startswith("http") else (self.URL_scraped + href)
                soup2 = self._open_and_soup(url)
                if soup2.find("form", {"name": "ea_stupl"}) or soup2.find("select", {"name": "ASPP"}):
                    return
            except Exception:
                # try next candidate
                pass

        # 3) Last resort: follow the first links on the page hoping the app pushes us to the overview
        for a in soup.find_all("a"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            try:
                url = href if href.startswith("http") else (self.URL_scraped + href)
                soup3 = self._open_and_soup(url)
                if soup3.find("form", {"name": "ea_stupl"}) or soup3.find("select", {"name": "ASPP"}):
                    return
            except Exception:
                continue
        # If we reach here, infos() will raise a clean error.

    # ---------------------------------------------------------------------
    # Main data fetch
    # ---------------------------------------------------------------------

    def infos(self) -> Dict[str, Any]:
        """
        Scrape study plan points (pp) and the LV list per pp.
        Returns e.g.:
          {
            "studies_count": N,
            "pp": {
               "<pp_id>": {
                   "id": "...", "name": "...", "lvs": {
                       "<lv_id>": { "name": "...", "prof": "...", ... }
                   }
               }, ...
            }
          }
        """
        self.data = {}

        # Make sure we are on the right page/view
        self.ensure_overview()

        # Select study-plan form; fallback to any form containing ASPP control
        selected = False
        try:
            self.browser.select_form("ea_stupl")
            selected = True
        except mechanize.FormNotFoundError:
            for frm in self.browser.forms():
                try:
                    self.browser.form = frm
                    _ = self.browser.form.find_control("ASPP")
                    selected = True
                    break
                except Exception:
                    continue

        if not selected:
            raise RuntimeError("Could not find study-plan form (ea_stupl / ASPP) after login.")

        # Preselect first ASPP option if present
        try:
            item = self.browser.form.find_control("ASPP").get(None, None, None, 0)
            item.selected = True
        except Exception:
            pass

        r = self.browser.submit()
        soup = BeautifulSoup(r.read(), "html.parser")

        # -------- studies (ASPP options) ----------
        studies: Dict[int, Dict[str, Any]] = {}
        select = soup.find("select", {"name": "ASPP"})
        if select:
            all_opts = select.find_all("option")
            for i, entry in enumerate(all_opts):
                text = entry.text or ""
                parts = text.split("/")
                if len(parts) == 1:
                    studies[i] = {
                        "id": entry.get("value", ""),
                        "title": entry.get("title", ""),
                        "name": text,
                        "abschnitte": {},
                    }
                elif len(parts) == 2 and (i - 1) % max(len(studies), 1) in studies:
                    parent_idx = (i - 1) % max(len(studies), 1)
                    parent = studies.get(parent_idx, {})
                    abschn = parent.setdefault("abschnitte", {})
                    abschn[entry.get("value", "")] = {
                        "id": entry.get("value", ""),
                        "title": entry.get("title", ""),
                        "name": text,
                    }
        self.data["studies_count"] = len(studies)

        # -------- planpunkte / lvs ----------
        pp: Dict[str, Dict[str, Any]] = {}
        table = soup.find("table", {"class": "b3k-data"})
        tbody = table.find("tbody") if table else None
        rows = tbody.find_all("tr") if tbody else []

        for i, planpunkt in enumerate(rows):
            # second column must have text to be a valid row
            second_td = planpunkt.select_one("td:nth-of-type(2)")
            if not (second_td and (second_td.text or "").strip()):
                continue

            a_tag = planpunkt.find("a")
            if not (a_tag and a_tag.get("id")):
                continue

            key = a_tag["id"][1:]  # drop leading 'S' (e.g. 'S12345' -> '12345')
            pp[key] = {}
            pp[key]["order"] = i + 1

            # parse hierarchy depth from style attribute (e.g. padding-left multiple of 16px)
            style_td = planpunkt.select_one("td:nth-of-type(1)")
            style_attr = style_td.get("style", "") if style_td else ""
            depth_nums = re.findall(r"\d+", style_attr)
            try:
                pp[key]["depth"] = int(depth_nums[0]) // 16
            except Exception:
                pp[key]["depth"] = 0

            pp[key]["id"] = key

            # type + name
            span1 = planpunkt.select_one("td:nth-of-type(1) span:nth-of-type(1)")
            span2 = planpunkt.select_one("td:nth-of-type(1) span:nth-of-type(2)")
            pp[key]["type"] = (span1.text if span1 else "").strip()
            pp[key]["name"] = (span2.text if span2 else "").strip()

            # LV/PRF urls & status
            link_lv = planpunkt.select_one('a[href*="DLVO"]')
            link_gp = planpunkt.select_one('a[href*="GP"]')
            if link_lv:
                pp[key]["lv_url"] = link_lv.get("href", "")
                pp[key]["lv_status"] = (link_lv.text or "").strip()
            if link_gp:
                pp[key]["prf_url"] = link_gp.get("href", "")

            # attempts (like '1/3') if present
            txt2 = (second_td.text or "").strip()
            if "/" in txt2:
                spans = planpunkt.select("td:nth-of-type(2) span")
                if len(spans) >= 2:
                    pp[key]["attempts"] = (spans[0].text or "").strip()
                    pp[key]["attempts_max"] = (spans[1].text or "").strip()

            # result/date if present
            td3 = planpunkt.select_one("td:nth-of-type(3)")
            td4 = planpunkt.select_one("td:nth-of-type(4)")
            if td3 and td3.text.strip():
                pp[key]["result"] = td3.text.strip()
            if td4 and td4.text.strip():
                pp[key]["date"] = td4.text.strip()

            # load LV list if available
            if "lv_url" in pp[key]:
                r2 = self.browser.open(self.URL_scraped + pp[key]["lv_url"])
                soup2 = BeautifulSoup(r2.read(), "html.parser")
                pp[key]["lvs"] = {}

                lv_table = soup2.find("table", {"class": "b3k-data"})
                lv_body = lv_table.find("tbody") if lv_table else None
                if lv_body:
                    for lv in lv_body.find_all("tr"):
                        ver_id_link = lv.select_one(".ver_id a")
                        if not ver_id_link:
                            continue
                        number = self._clean_text(ver_id_link.text)
                        if not number:
                            continue

                        cur: Dict[str, Any] = {}
                        pp[key]["lvs"][number] = cur
                        cur["id"] = number

                        # semester (e.g., "WiSe 2025", "SoSe 2025", etc.)
                        sem_span = lv.select_one(".ver_id span")
                        cur["semester"] = self._clean_text(sem_span.text) if sem_span else None

                        # lecturer(s)
                        prof_div = lv.select_one(".ver_title div")
                        cur["prof"] = self._clean_text(prof_div.get_text(" ", strip=True) if prof_div else "")

                        # --- robust course title extraction ---
                        name_td = lv.find("td", {"class": "ver_title"})
                        title_text = ""
                        if name_td:
                            # Prefer a clear title element; else fallback to the whole cell text
                            title_el = name_td.select_one("a, strong, span")
                            if title_el:
                                title_text = self._clean_text(title_el.get_text(" ", strip=True))
                            else:
                                title_text = self._clean_text(name_td.get_text(" ", strip=True))

                            # If lecturer text appears inside the same cell, strip it from the end
                            if cur["prof"]:
                                # Remove trailing lecturer text and leftover separators
                                title_text = re.sub(
                                    re.escape(cur["prof"]) + r"\s*$",
                                    "",
                                    title_text,
                                ).strip(" -·•\u00A0")

                            # As a last resort, remove any repeated whitespace or stray separators
                            title_text = self._clean_text(title_text)

                        cur["name"] = title_text

                        # status
                        status_div = lv.select_one("td.box div")
                        cur["status"] = self._clean_text(status_div.text) if status_div else None

                        # capacity/free (format like "x / y")
                        cap_div = lv.select_one('div[class*="capacity_entry"]')
                        cap_txt = self._clean_text(cap_div.text) if cap_div else ""
                        try:
                            slash = cap_txt.rindex("/")
                            free = cap_txt[:slash].strip()
                            cap = cap_txt[slash + 1:].strip()
                            cur["free"] = int(re.sub(r"[^\d]", "", free)) if free else None
                            cur["capacity"] = int(re.sub(r"[^\d]", "", cap)) if cap else None
                        except Exception:
                            cur["free"] = None
                            cur["capacity"] = None

                        # internal id from form name
                        form = lv.select_one("td.action form")
                        if form and form.get("name"):
                            internal_id = form["name"]
                            if "_" in internal_id:
                                cur["internal_id"] = internal_id.rsplit("_", 1)[-1]

                        # registration time window
                        ts_span = lv.select_one("td.action .timestamp span")
                        date_txt = self._clean_text(ts_span.text) if ts_span else ""
                        if date_txt.startswith("ab "):
                            cur["date_start"] = date_txt[3:].strip()
                        if date_txt.startswith("bis "):
                            cur["date_end"] = date_txt[4:].strip()

                        # already registered?
                        reg_box = lv.select_one("td.box.active .timestamp span")
                        if reg_box and reg_box.text:
                            cur["registered_at"] = self._clean_text(reg_box.text)

                        # waitlist present?
                        wl_div = lv.select_one('td.capacity div[title*="Anzahl Warteliste"]')
                        if wl_div:
                            span = wl_div.find("span")
                            cur["waitlist"] = self._clean_text(span.text if span else wl_div.text)

        self.data["pp"] = pp
        return self.data

    # ---------------------------------------------------------------------
    # Compatibility wrapper used by your FastAPI code
    # ---------------------------------------------------------------------

    def getResults(self) -> Dict[str, Any]:
        """
        Return a normalized structure compatible with the original project:
            { "data": <self.data>, "status": {...} }
        Ensures infos() ran at least once.
        """
        if not self.data or "pp" not in self.data:
            self.infos()

        status = dict(self.status)
        ts = status.get("last_logged_in")
        if isinstance(ts, datetime.datetime):
            status["last_logged_in"] = ts.strftime("%Y-%m-%d %H:%M:%S")

        return {
            "data": self.data,
            "status": status,
        }
