#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import re
import time
import base64
import pickle
import hashlib
import datetime
import threading
from datetime import timedelta
from dateutil import parser
from typing import Any, Dict

import requests  # if unused, you can remove
from lxml import html
from bs4 import BeautifulSoup
import mechanize


class WuLpisApi:
    URL = "https://lpis.wu.ac.at/lpis"

    def __init__(self, username=None, password=None, args=None, sessiondir=None):
        self.username = username or ""
        self.password = password or ""
        # expects usernames like h12345678; matr_nr is the part after the first char
        self.matr_nr = self.username[1:] if self.username else ""
        self.args = args
        self.data: Dict[str, Any] = {}
        self.status: Dict[str, Any] = {}

        self.browser = mechanize.Browser()
        self.browser.set_handle_robots(False)   # ignore robots
        self.browser.set_handle_refresh(False)  # can sometimes hang without this
        self.browser.set_handle_equiv(True)
        self.browser.set_handle_redirect(True)
        self.browser.set_handle_referer(True)
        self.browser.set_debug_http(False)
        self.browser.set_debug_responses(False)
        self.browser.set_debug_redirects(False)
        self.browser.addheaders = [
            ('User-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                           '(KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'),
            ('Accept', '*/*')
        ]

        # sessions/<username> as default (next to the process)
        if sessiondir:
            self.sessionfile = os.path.join(sessiondir, self.username)
        else:
            self.sessionfile = os.path.join("sessions", self.username)

        self.login()

    # --------------------------- utils ---------------------------

    @staticmethod
    def _read_bytes_to_str(resp_bytes: bytes) -> str:
        # Make sure regex/lxml/bs get text
        if isinstance(resp_bytes, bytes):
            return resp_bytes.decode("utf-8", errors="ignore")
        return str(resp_bytes or "")

    # --------------------------- session ---------------------------

    def save_session(self):
        """Persist the mechanize browser (pickle)."""
        try:
            os.makedirs(os.path.dirname(self.sessionfile), exist_ok=True)
            with open(self.sessionfile, 'wb') as f:
                pickle.dump(self.browser, f, pickle.HIGHEST_PROTOCOL)
            return True
        except Exception as e:
            print(f"[save_session] failed: {e}")
            return False

    def load_session(self):
        """Load a previously saved browser session."""
        try:
            if os.path.isfile(self.sessionfile):
                with open(self.sessionfile, 'rb') as f:
                    self.browser = pickle.load(f)
                return True
            return False
        except Exception as e:
            print(f"[load_session] failed: {e}")
            return False

    # --------------------------- login ---------------------------

   def login(self):
    # Reset
    self.data = {}

    # Open login page
    r = self.browser.open(self.URL)

    # Select the login form (fallback to first form if name changed)
    try:
        self.browser.select_form('login')
    except mechanize.FormNotFoundError:
        forms = list(self.browser.forms())
        if not forms:
            raise RuntimeError("Login form not found on LPIS landing page.")
        self.browser.form = forms[0]

    # Read raw BYTES and clean comments as BYTES (important for lxml)
    raw = r.read()  # bytes
    cleaned = re.sub(rb"<!--.*?-->", b"", raw, flags=re.S)

    # Parse with lxml FROM BYTES to avoid 'Unicode strings with encoding declaration...' error
    tree = html.fromstring(cleaned)

    # Find username/password input names by accesskey
    usernames = list(set(tree.xpath("//input[@accesskey='u']/@name")))
    passwords = list(set(tree.xpath("//input[@accesskey='p']/@name")))
    if not usernames or not passwords:
        raise RuntimeError("Could not locate LPIS username/password fields on login page.")

    input_username = usernames[0]
    input_password = passwords[0]

    # Fill & submit
    self.browser[input_username] = self.username
    self.browser[input_password] = self.password
    r = self.browser.submit()

    # Extract the base URL after login (e.g., https://lpis.wu.ac.at/kdcs/bach-s##/#####/)
    url = r.geturl()
    if "/" not in url:
        raise RuntimeError("Unexpected LPIS redirect URL after login.")
    self.URL_scraped = url[:url.rindex('/') + 1]

    self.data = self.URL_scraped
    self.status["last_logged_in"] = datetime.datetime.now()
    return self.data

    # --------------------------- infos (structure) ---------------------------

    def infos(self):
        """Scrape study plan points (pp) and course list (lvs) per pp."""
        # print("getting data ...")
        self.data = {}

        # select form for study plan overview
        try:
            self.browser.select_form('ea_stupl')
        except mechanize.FormNotFoundError:
            # try to navigate to start page again
            self.browser.open(self.URL_scraped)
            self.browser.select_form('ea_stupl')

        form = self.browser.form
        # Select first element in Select Options Dropdown: ASPP
        try:
            item = form.find_control("ASPP").get(None, None, None, 0)
            item.selected = True
        except Exception:
            pass  # if not present, continue

        r = self.browser.submit()
        soup = BeautifulSoup(r.read(), "html.parser")

        # -------- studies (ASPP options) ----------
        studies = {}
        select = soup.find('select', {'name': 'ASPP'})
        if select:
            all_opts = select.find_all('option')
            for i, entry in enumerate(all_opts):
                text = entry.text or ""
                parts = text.split('/')
                if len(parts) == 1:
                    studies[i] = {
                        'id': entry.get('value', ''),
                        'title': entry.get('title', ''),
                        'name': text,
                        'abschnitte': {}
                    }
                elif len(parts) == 2 and (i - 1) % max(len(studies), 1) in studies:
                    parent_idx = (i - 1) % max(len(studies), 1)
                    abschn = studies.get(parent_idx, {}).setdefault('abschnitte', {})
                    abschn[entry.get('value', '')] = {
                        'id': entry.get('value', ''),
                        'title': entry.get('title', ''),
                        'name': text
                    }
        self.data['studies'] = studies

        # -------- planpunkte / lvs ----------
        pp = {}
        table = soup.find('table', {"class": "b3k-data"})
        tbody = table.find('tbody') if table else None
        rows = tbody.find_all('tr') if tbody else []

        for i, planpunkt in enumerate(rows):
            # second td has text → valid row
            second_td = planpunkt.select_one('td:nth-of-type(2)')
            if not (second_td and (second_td.text or "").strip()):
                continue

            a_tag = planpunkt.find('a')
            if not (a_tag and a_tag.get('id')):
                continue

            key = a_tag['id'][1:]  # drop leading char (e.g. 'S12345' -> '12345')
            pp[key] = {}
            pp[key]["order"] = i + 1

            style_td = planpunkt.select_one('td:nth-of-type(1)')
            style_attr = style_td.get('style', '') if style_td else ''
            depth_nums = re.findall(r'\d+', style_attr)
            try:
                pp[key]["depth"] = int(depth_nums[0]) // 16
            except Exception:
                pp[key]["depth"] = 0

            pp[key]["id"] = key

            # type + name
            span1 = planpunkt.select_one('td:nth-of-type(1) span:nth-of-type(1)')
            span2 = planpunkt.select_one('td:nth-of-type(1) span:nth-of-type(2)')
            pp[key]["type"] = (span1.text if span1 else "").strip()
            pp[key]["name"] = (span2.text if span2 else "").strip()

            # lv/prf urls & status
            link_lv = planpunkt.select_one('a[href*="DLVO"]')
            link_gp = planpunkt.select_one('a[href*="GP"]')
            if link_lv:
                pp[key]["lv_url"] = link_lv.get('href', '')
                pp[key]["lv_status"] = (link_lv.text or "").strip()
            if link_gp:
                pp[key]["prf_url"] = link_gp.get('href', '')

            # attempts (like '1/3')
            txt2 = (second_td.text or "").strip()
            if '/' in txt2:
                spans = planpunkt.select('td:nth-of-type(2) span')
                if len(spans) >= 2:
                    pp[key]["attempts"] = (spans[0].text or "").strip()
                    pp[key]["attempts_max"] = (spans[1].text or "").strip()

            # result/date
            td3 = planpunkt.select_one('td:nth-of-type(3)')
            td4 = planpunkt.select_one('td:nth-of-type(4)')
            if td3 and td3.text.strip():
                pp[key]["result"] = td3.text.strip()
            if td4 and td4.text.strip():
                pp[key]["date"] = td4.text.strip()

            # load LV list if available
            if "lv_url" in pp[key]:
                r2 = self.browser.open(self.URL_scraped + pp[key]["lv_url"])
                soup2 = BeautifulSoup(r2.read(), "html.parser")
                pp[key]['lvs'] = {}

                lv_table = soup2.find('table', {"class": "b3k-data"})
                lv_body = lv_table.find('tbody') if lv_table else None
                if lv_body:
                    for lv in lv_body.find_all('tr'):
                        ver_id_link = lv.select_one('.ver_id a')
                        if not ver_id_link:
                            continue
                        number = (ver_id_link.text or "").strip()
                        if not number:
                            continue

                        pp[key]['lvs'][number] = {}
                        cur = pp[key]['lvs'][number]
                        cur['id'] = number

                        sem_span = lv.select_one('.ver_id span')
                        cur['semester'] = (sem_span.text or "").strip() if sem_span else None

                        prof_div = lv.select_one('.ver_title div')
                        cur['prof'] = (prof_div.text or "").strip() if prof_div else ""

                        name_td = lv.find('td', {"class": "ver_title"})
                        if name_td:
                            # text nodes not inside children
                            name_texts = [t for t in name_td.find_all(text=True, recursive=False)]
                            cur['name'] = (name_texts[1] if len(name_texts) > 1 else name_texts[0] if name_texts else "").strip()
                        else:
                            cur['name'] = ""

                        status_div = lv.select_one('td.box div')
                        cur['status'] = (status_div.text or "").strip() if status_div else None

                        cap_div = lv.select_one('div[class*="capacity_entry"]')
                        cap_txt = (cap_div.text or "").strip() if cap_div else ""
                        # format "x / y" → parse defensively
                        try:
                            slash = cap_txt.rindex('/')
                            free = cap_txt[:slash].strip()
                            cap = cap_txt[slash + 1:].strip()
                            cur['free'] = int(re.sub(r'[^\d]', '', free)) if free else None
                            cur['capacity'] = int(re.sub(r'[^\d]', '', cap)) if cap else None
                        except Exception:
                            cur['free'] = None
                            cur['capacity'] = None

                        # internal id from form name
                        form = lv.select_one('td.action form')
                        if form and form.get('name'):
                            internal_id = form['name']
                            if '_' in internal_id:
                                cur['internal_id'] = internal_id.rsplit('_', 1)[-1]

                        # registration time window
                        ts_span = lv.select_one('td.action .timestamp span')
                        date_txt = (ts_span.text or "").strip() if ts_span else ""
                        if date_txt.startswith('ab '):
                            cur['date_start'] = date_txt[3:].strip()
                        if date_txt.startswith('bis '):
                            cur['date_end'] = date_txt[4:].strip()

                        # already registered?
                        reg_box = lv.select_one('td.box.active .timestamp span')
                        if reg_box and reg_box.text:
                            cur['registered_at'] = reg_box.text.strip()

                        # waitlist present?
                        wl_div = lv.select_one('td.capacity div[title*="Anzahl Warteliste"]')
                        if wl_div:
                            span = wl_div.find('span')
                            cur['waitlist'] = (span.text or "").strip() if span else (wl_div.text or "").strip()

        self.data['pp'] = pp
        return self.data

    # --------------------------- registration (legacy) ---------------------------

    def registration(self):
        """Legacy interactive registration logic (kept mostly as-is, Py3 prints)."""
        try:
            self.browser.select_form('ea_stupl')
        except mechanize.FormNotFoundError:
            self.browser.open(self.URL_scraped)
            self.browser.select_form('ea_stupl')

        form = self.browser.form
        try:
            item = form.find_control("ASPP").get(None, None, None, 0)
            item.selected = True
        except Exception:
            pass

        # offset before trigger time
        offset = 1.0  # seconds
        if self.args and getattr(self.args, "planobject", None) and getattr(self.args, "course", None):
            pp = "S" + self.args.planobject
            lv = self.args.course
            lv2 = getattr(self.args, "course2", None)
        else:
            raise RuntimeError("registration() requires args.planobject and args.course")

        self.data = {}
        self.browser.select_form('ea_stupl')
        r = self.browser.submit()
        soup = BeautifulSoup(r.read(), "html.parser")

        # open LV list page for the plan point
        link = soup.find('table', {"class": "b3k-data"}).find('a', id=pp)
        if not link:
            raise RuntimeError("Planpunkt link not found.")
        url = link.parent.find('a', href=True)["href"]
        r = self.browser.open(self.URL_scraped + url)

        triggertime = 0
        soup = BeautifulSoup(r.read(), "html.parser")
        ts_span = soup.find('table', {"class": "b3k-data"}).find('a', text=lv).parent.parent.select_one('.action .timestamp span')
        date_txt = ts_span.text.strip() if ts_span and ts_span.text else ""
        if date_txt.startswith('ab '):
            triggetime = time.mktime(datetime.datetime.strptime(date_txt[3:], "%d.%m.%Y %H:%M").timetuple()) - offset
            if triggertime > time.time():
                secs = triggertime - time.time()
                print(f"waiting: {secs:.2f} seconds ({secs/60:.2f} minutes)")
                print(f"waiting till: {triggertime} ({time.strftime('%d.%m.%Y %H:%M:%S', time.localtime(triggertime))})")
                time.sleep(secs)

        print(f"triggertime: {triggertime}")
        print(f"final open time start: {datetime.datetime.now()}")

        # Reload page until registration is possible
        while True:
            print(f"start request {datetime.datetime.now()}")
            r = self.browser.open(self.URL_scraped + url)
            soup = BeautifulSoup(r.read(), "html.parser")

            possible = soup.find('table', {"class": "b3k-data"}).find('a', text=lv).parent.parent.select('div.box.possible')
            if possible:
                break
            else:
                print(f"parsing done {datetime.datetime.now()}")
            print("registration is not (yet) possible, waiting ...")
            print("reloading page and waiting for form to be submittable")

        print(f"final open time end: {datetime.datetime.now()}")
        print("registration is possible")

        cap1_div = soup.find('table', {"class": "b3k-data"}).find('a', text=lv).parent.parent.select('div[class*="capacity_entry"]')
        cap2_div = soup.find('table', {"class": "b3k-data"}).find('a', text=lv2).parent.parent.select('div[class*="capacity_entry"]') if lv2 else None
        cap1 = cap1_div[0].text.strip() if cap1_div else "0 / 0"
        cap2 = cap2_div[0].text.strip() if cap2_div else "0 / 0"

        def _parse_free(s):
            try:
                slash = s.rindex('/')
                return int(re.sub(r'[^\d]', '', s[:slash]))
            except Exception:
                return 0

        free1 = _parse_free(cap1)
        free2 = _parse_free(cap2) if lv2 else 0

        form1 = soup.find('table', {"class": "b3k-data"}).find('a', text=lv).parent.parent.select_one('.action form')
        form2 = soup.find('table', {"class": "b3k-data"}).find('a', text=lv2).parent.parent.select_one('.action form') if lv2 else None

        name1 = form1["name"].strip() if form1 and form1.get("name") else None
        name2 = form2["name"].strip() if form2 and form2.get("name") else None

        print(f"end time: {datetime.datetime.now()}")
        print(f"freie plaetze: lv1: {free1}, lv2: {free2} (if defined)")

        if name1 is None and name2 is None:
            raise RuntimeError("No registration forms found.")

        if free1 > 0 and name1:
            self.browser.select_form(name1)
            print(f"submitting registration form1 ({name1})")
        elif name2:
            self.browser.select_form(name2)
            print(f"submitting registration form2 ({name2})")

        r = self.browser.submit()
        soup = BeautifulSoup(r.read(), "html.parser")
        alert = soup.find('div', {"class": 'b3k_alert_content'})
        if alert:
            print(alert.text.strip())
            lv_row = soup.find('table', {"class": "b3k-data"}).find('a', text=lv).parent.parent
            cap_txt = lv_row.select_one('div[class*="capacity_entry"]')
            if cap_txt:
                print("Frei: " + cap_txt.text.strip())
            wl = lv_row.select_one('td.capacity div[title*="Anzahl Warteliste"] span')
            if wl:
                wltxt = wl.text.strip()
                print(f"Warteliste: {wltxt} / {wltxt}")
                if free1 > 0 and name2:
                    self.browser.select_form(name2)
                    print(f"submitting registration form ({name2})")
                    r = self.browser.submit()

        h3 = soup.find('h3')
        if h3 and h3.find('span'):
            print(h3.find('span').text.strip())
