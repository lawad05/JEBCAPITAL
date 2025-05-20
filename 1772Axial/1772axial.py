import os
import time
import random
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# Dummy form details for directory access (no longer auto-filled)
ACCESS_DETAILS = {
    'First Name':    'Jane',
    'Last Name':     'Doe',
    'Email':         'jane.doe@example.com',
    'Phone Number':  '555-1234',
    'Company Name':  'Acme LLC',
    'Company Type':  'M&A Advisory',
    'Visit Reason':  'researching potential buyers'
}

# Rotate through a small pool of User-Agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Safari/605.1.15'
]

class AxialScraper:
    def __init__(self, debug=True):
        self.debug = debug
        # Updated to M&A Advisory Firms page
        self.url = 'https://www.axial.net/forum/companies/m-a-advisory-firms/'
        # Updated default output filename
        self.excel_path = os.path.join(os.getcwd(), 'axial_m_a_advisory_firms.xlsx')
        self.save_frequency = 5
        self.data = []
        self.scraped_companies = set()
        self.driver = None

        if os.path.exists(self.excel_path):
            df = pd.read_excel(self.excel_path)
            self.scraped_companies = set(df['Company Name'])
            self.existing_df = df
            print(f"Loaded {len(self.scraped_companies)} previously scraped companies")
        else:
            self.existing_df = pd.DataFrame(columns=[
                'Company Name','Website','Location','Team Member','Industry'
            ])
            print('Starting fresh — no existing Excel file found')

    def setup_driver(self):
        chrome_opts = Options()
        if not self.debug:
            chrome_opts.add_argument('--headless')
        chrome_opts.add_argument('--no-sandbox')
        chrome_opts.add_argument('--disable-dev-shm-usage')
        chrome_opts.add_argument('--disable-gpu')
        chrome_opts.add_argument('--window-size=1920,1080')
        chrome_opts.add_experimental_option('prefs', {
            'profile.managed_default_content_settings.images': 2
        })
        chrome_opts.page_load_strategy = 'normal'
        chrome_opts.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")

        svc = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=svc, options=chrome_opts)
        self.driver.implicitly_wait(10)
        print(f'WebDriver setup complete (headless={not self.debug})')

    def handle_cookies(self):
        try:
            btn = WebDriverWait(self.driver, 3).until(
                EC.element_to_be_clickable((By.XPATH,
                    "//button[contains(., 'Reject Cookies') or contains(., 'Accept Cookies')]"
                ))
            )
            btn.click()
            time.sleep(0.5)
        except TimeoutException:
            pass

    def remove_overlay(self):
        self.driver.execute_script(
            "document.querySelectorAll('.cky-overlay').forEach(el=>el.remove());"
        )

    def save_progress(self, force=False):
        count = len(self.data)
        if count == 0:
            return
        if not force and count < self.save_frequency:
            return
        df_new = pd.DataFrame(self.data)
        combined = pd.concat([self.existing_df, df_new], ignore_index=True)
        combined.to_excel(self.excel_path, index=False)
        self.existing_df = combined
        self.scraped_companies |= set(df_new['Company Name'])
        self.data.clear()
        print(f"→ Saved {len(combined)} companies")

    def get_website(self):
        try:
            el = self.driver.find_element(By.XPATH, "//form/div[3]/p/a")
            return el.get_attribute('href')
        except:
            return 'Not available'

    def get_location(self):
        try:
            el = self.driver.find_element(By.XPATH, "//form/div[2]/p/span[1]")
            txt = el.text.strip()
            if txt:
                return txt
        except:
            pass
        m = re.search(r'location["\s:]+([^,"<]+)', self.driver.page_source, re.IGNORECASE)
        return m.group(1).strip() if m else 'Not specified'

    def get_team(self):
        try:
            return self.driver.find_element(
                By.XPATH, "//axl-account-profile-member[1]//p[1]"
            ).text.strip()
        except:
            return 'Not specified'

    def scrape_page(self, idx):
        self.handle_cookies()
        self.remove_overlay()
        items = self.driver.find_elements(By.XPATH, "//a[@itemprop='name']")
        new = [(e.text, e.get_attribute('href')) for e in items if e.text and e.text not in self.scraped_companies]
        print(f"\n=== Page {idx}: found {len(new)} firms ===")
        for name, url in new:
            print(f"→ {name} | {url}")
            self.driver.get(url)
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//form/div[3]/p/a"))
                )
            except TimeoutException:
                print(f"⚠ timeout waiting for profile: {name}")
            self.handle_cookies()
            self.remove_overlay()
            site = self.get_website()
            loc  = self.get_location()
            team = self.get_team()
            m    = re.search(r'industry["\s:]+([^,"<]+)', self.driver.page_source, re.IGNORECASE)
            ind  = m.group(1).strip() if m else 'M&A Advisory'
            self.data.append({
                'Company Name': name,
                'Website': site,
                'Location': loc,
                'Team Member': team,
                'Industry': ind
            })
            print(f"   ✓ {site} | {loc} | {team}")
            self.save_progress()
            time.sleep(random.uniform(0.5, 1.0))
            self.driver.back()

    def run(self):
        self.setup_driver()
        self.driver.get(self.url)
        print(f"Loaded {self.url}\n")
        print("🚧 Please complete the directory-access form in the browser now.")
        input("    When you’re done, press ENTER here to start scraping…")

        page = 1
        while True:
            self.scrape_page(page)
            try:
                nxt = self.driver.find_element(By.LINK_TEXT, str(page + 1))
                nxt.click()
            except:
                break
            page += 1

        self.save_progress(force=True)
        print("Done!")
        self.driver.quit()

if __name__ == '__main__':
    scraper = AxialScraper(debug=True)
    scraper.run()
