import os
import time
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager

# Rotate through User-Agents for better scraping reliability
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/112.0'
]

class BusinessBrokerScraper:
    def __init__(self, debug=True):
        self.debug = debug
        self.base_url = 'https://www.businessbroker.net/brokers/brokers.aspx'
        self.output_dir = os.path.join(os.getcwd(), 'businessbroker')
        self.excel_path = os.path.join(self.output_dir, 'business_brokers.xlsx')
        self.save_frequency = 5
        self.data = []
        self.scraped_brokers = set()
        self.driver = None
        self.processed_urls = set()  # Track processed URLs to avoid duplicates

        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"Created output directory: {self.output_dir}")

        # Load existing data if available
        if os.path.exists(self.excel_path):
            df = pd.read_excel(self.excel_path)
            # Track scraped brokers by their URL to avoid duplicates
            if 'Website' in df.columns:
                self.processed_urls = set(df['Website'].dropna().tolist())
            self.existing_df = df
            print(f"Loaded {len(df)} previously scraped brokers")
        else:
            self.existing_df = pd.DataFrame(columns=[
                'Broker Number', 'Broker Name', 'Company Name', 'Website'
            ])
            print('Starting fresh — no existing Excel file found')

    def setup_driver(self):
        chrome_opts = Options()
        if not self.debug:
            chrome_opts.add_argument('--headless')
        chrome_opts.add_argument('--no-sandbox')
        chrome_opts.add_argument('--disable-dev-shm-usage')
        chrome_opts.add_argument('--disable-gpu')
        chrome_opts.add_argument('--start-maximized')  # Start maximized
        chrome_opts.add_experimental_option('prefs', {
            'profile.managed_default_content_settings.images': 2  # Don't load images
        })
        chrome_opts.page_load_strategy = 'normal'
        chrome_opts.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")

        svc = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=svc, options=chrome_opts)
        self.wait = WebDriverWait(self.driver, 10)
        print(f'WebDriver setup complete (headless={not self.debug})')

    def scroll_to_element(self, element):
        """Scroll element into view using JavaScript"""
        self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
        time.sleep(0.5)  # Give time for any animations to complete

    def click_with_retry(self, element, max_attempts=3):
        """Attempt to click an element with multiple retry strategies"""
        for attempt in range(max_attempts):
            try:
                # Scroll the element into view
                self.scroll_to_element(element)
                
                # Try different click strategies
                if attempt == 0:
                    element.click()
                elif attempt == 1:
                    ActionChains(self.driver).move_to_element(element).click().perform()
                else:
                    self.driver.execute_script("arguments[0].click();", element)
                return True
            except Exception as e:
                print(f"Click attempt {attempt + 1} failed: {str(e)}")
                time.sleep(1)
        return False

    def handle_cookies(self):
        try:
            # Look for the Allow button in the cookie consent dialog
            cookie_btn = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[text()='Allow']"))
            )
            cookie_btn.click()
            time.sleep(1)
        except TimeoutException:
            print("No cookie consent dialog found or already accepted")
            pass

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
        self.data.clear()
        print(f"→ Saved {len(combined)} brokers")

    def get_states(self):
        """Get list of all states with retry mechanism"""
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                # Navigate to the base URL
                self.driver.get(self.base_url)
                time.sleep(random.uniform(2, 5))
                
                # Handle any cookie consent dialogs
                self.handle_cookies()
                
                # Wait for the USA section header
                usa_header = self.wait.until(
                    EC.presence_of_element_located((By.XPATH, "//h3[contains(text(), 'United States of America')]"))
                )
                self.scroll_to_element(usa_header)
                
                # Wait for state links to be present
                state_links = self.wait.until(
                    EC.presence_of_all_elements_located((By.XPATH, "//h3[contains(text(), 'United States of America')]/following-sibling::ul[1]/li/a"))
                )
                
                # Store state names and URLs
                states = []
                for link in state_links:
                    try:
                        name = link.text.strip()
                        url = link.get_attribute('href')
                        if name and url:
                            states.append({'name': name, 'url': url})
                            print(f"Found state: {name}")
                    except StaleElementReferenceException:
                        continue
                
                print(f"Found {len(states)} states")
                return states
            except Exception as e:
                print(f"Attempt {attempt + 1} to get states failed: {str(e)}")
                if attempt < max_attempts - 1:
                    print("Refreshing page and retrying...")
                    self.driver.refresh()
                    time.sleep(random.uniform(2, 5))
        return []

    def get_broker_listings(self, state_url):
        """Get all broker listings from a state page"""
        try:
            # Navigate to state URL
            self.driver.get(state_url)
            time.sleep(random.uniform(2, 5))
            
            # Find all broker listings
            broker_listings = []
            
            # Using a more general XPath pattern based on the provided example
            # The pattern looks for all div elements that might contain broker listings
            try:
                # First, find all potential broker container divs
                broker_containers = self.driver.find_elements(By.XPATH, "//main/div[4]/div[3]/div/div")
                print(f"Found {len(broker_containers)} potential broker containers")
                
                # For each container, look for the "View broker profile" button
                for idx, container in enumerate(broker_containers):
                    try:
                        # Try to find the view profile button using a more specific XPath
                        # This is based on the example XPath: /html/body/main/div[4]/div[3]/div/div[34]/div[2]/p[4]/a/span
                        # But generalized to work with any div index
                        profile_buttons = container.find_elements(By.XPATH, ".//div[2]/p[4]/a/span[contains(text(), 'View broker profile')]")
                        
                        # If that doesn't work, try a more general approach
                        if not profile_buttons:
                            profile_buttons = container.find_elements(By.XPATH, ".//a[contains(., 'View broker profile')]")
                        
                        # If that still doesn't work, try an even more general approach
                        if not profile_buttons:
                            profile_buttons = container.find_elements(By.XPATH, ".//a[contains(@href, 'broker')]")
                        
                        for button in profile_buttons:
                            # Get the parent <a> tag that contains the href
                            parent_a = button
                            if button.tag_name != 'a':
                                parent_a = button.find_element(By.XPATH, "./ancestor::a")
                            
                            url = parent_a.get_attribute('href')
                            if url and url not in self.processed_urls:
                                broker_listings.append(url)
                                print(f"Found broker profile #{idx}: {url}")
                    except Exception as e:
                        print(f"Error processing broker container {idx}: {str(e)}")
                
                # If we still haven't found any listings, try a completely different approach
                if not broker_listings:
                    print("Trying alternative approach to find broker listings...")
                    # Look for any links that might be broker profile links
                    all_links = self.driver.find_elements(By.XPATH, "//a[contains(@href, 'broker')]")
                    for link in all_links:
                        url = link.get_attribute('href')
                        if url and 'broker' in url and url not in self.processed_urls:
                            broker_listings.append(url)
                            print(f"Found broker profile (alt method): {url}")
                
                # Handle pagination
                self.process_pagination(broker_listings)
                
                return broker_listings
                
            except Exception as e:
                print(f"Error finding broker listings: {str(e)}")
                return []
                
        except Exception as e:
            print(f"Error getting broker listings: {str(e)}")
            return []

    def process_pagination(self, broker_listings):
        """Process pagination to get all broker listings"""
        page = 1
        while True:
            try:
                # Look for the next page button
                next_button = self.driver.find_element(By.XPATH, "//a[contains(text(), 'Next')]")
                if not next_button.is_displayed() or not next_button.is_enabled():
                    break
                
                page += 1
                print(f"Moving to page {page}")
                
                # Click the next button
                if not self.click_with_retry(next_button):
                    print(f"Failed to click next button for page {page}")
                    break
                
                # Wait for the page to load
                time.sleep(random.uniform(2, 5))
                
                # Find broker elements on the new page using the same approach as before
                try:
                    broker_containers = self.driver.find_elements(By.XPATH, "//main/div[4]/div[3]/div/div")
                    print(f"Found {len(broker_containers)} potential broker containers on page {page}")
                    
                    for idx, container in enumerate(broker_containers):
                        try:
                            # Try specific XPath first
                            profile_buttons = container.find_elements(By.XPATH, ".//div[2]/p[4]/a/span[contains(text(), 'View broker profile')]")
                            
                            # If that doesn't work, try more general approaches
                            if not profile_buttons:
                                profile_buttons = container.find_elements(By.XPATH, ".//a[contains(., 'View broker profile')]")
                            
                            if not profile_buttons:
                                profile_buttons = container.find_elements(By.XPATH, ".//a[contains(@href, 'broker')]")
                            
                            for button in profile_buttons:
                                # Get the parent <a> tag that contains the href
                                parent_a = button
                                if button.tag_name != 'a':
                                    parent_a = button.find_element(By.XPATH, "./ancestor::a")
                                
                                url = parent_a.get_attribute('href')
                                if url and url not in self.processed_urls:
                                    broker_listings.append(url)
                                    print(f"Found broker profile #{idx} on page {page}: {url}")
                        except Exception as e:
                            print(f"Error processing broker container {idx} on page {page}: {str(e)}")
                    
                except Exception as e:
                    print(f"Error finding broker listings on page {page}: {str(e)}")
                    break
                
            except NoSuchElementException:
                print("No more pages")
                break
            except Exception as e:
                print(f"Error processing pagination: {str(e)}")
                break

    def extract_broker_info(self, url):
        """Extract information from a broker profile page"""
        try:
            # Navigate to the broker profile page
            self.driver.get(url)
            time.sleep(random.uniform(2, 5))
            
            # Initialize broker info dictionary
            broker_info = {
                'Broker Number': 'Not found',
                'Broker Name': 'Not found',
                'Company Name': 'Not found',
                'Website': url  # Store the URL as the website
            }
            
            # Extract broker number using the provided XPath
            try:
                broker_number_element = self.driver.find_element(By.XPATH, "/html/body/main/div[1]/div/div/div[2]/div[1]/p")
                broker_info['Broker Number'] = broker_number_element.text.strip()
            except NoSuchElementException:
                # Try alternative XPath
                try:
                    broker_number_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'broker-number')]/p")
                    broker_info['Broker Number'] = broker_number_element.text.strip()
                except:
                    print("Broker number not found")
            
            # Extract broker name using the provided XPath
            try:
                broker_name_element = self.driver.find_element(By.XPATH, "/html/body/main/div[1]/div/div/table/tbody/tr/td/h1")
                broker_info['Broker Name'] = broker_name_element.text.strip()
            except NoSuchElementException:
                # Try alternative XPath
                try:
                    broker_name_element = self.driver.find_element(By.XPATH, "//h1[contains(@class, 'broker-name')]")
                    broker_info['Broker Name'] = broker_name_element.text.strip()
                except:
                    print("Broker name not found")
            
            # Extract company name using the provided XPath
            try:
                company_name_element = self.driver.find_element(By.XPATH, "/html/body/main/div[1]/div/div/div[1]/h2")
                broker_info['Company Name'] = company_name_element.text.strip()
            except NoSuchElementException:
                # Try alternative XPath
                try:
                    company_name_element = self.driver.find_element(By.XPATH, "//h2[contains(@class, 'company-name')]")
                    broker_info['Company Name'] = company_name_element.text.strip()
                except:
                    print("Company name not found")
            
            # Extract website using the provided XPath
            try:
                website_element = self.driver.find_element(By.XPATH, "/html/body/main/div[1]/div/div/div[2]/div[2]/a")
                website_url = website_element.get_attribute('href')
                if website_url:
                    broker_info['Website'] = website_url
            except NoSuchElementException:
                # Try alternative XPath
                try:
                    website_element = self.driver.find_element(By.XPATH, "//a[contains(@href, 'http') and not(contains(@href, 'businessbroker.net'))]")
                    website_url = website_element.get_attribute('href')
                    if website_url:
                        broker_info['Website'] = website_url
                except:
                    print("Website not found")
            
            print(f"Extracted broker info: {broker_info}")
            return broker_info
            
        except Exception as e:
            print(f"Error extracting broker info: {str(e)}")
            return None

    def run(self):
        """Main method to run the scraper"""
        try:
            # Setup the WebDriver
            self.setup_driver()
            
            # Get all states
            states = self.get_states()
            
            if not states:
                print("No states found. Exiting.")
                return
            
            # Process each state
            for state_idx, state in enumerate(states, 1):
                print(f"\nProcessing state {state_idx}/{len(states)}: {state['name']}")
                
                # Get all broker listings for this state
                broker_listings = self.get_broker_listings(state['url'])
                
                if not broker_listings:
                    print(f"No broker listings found for {state['name']}")
                    continue
                
                print(f"Found {len(broker_listings)} broker listings for {state['name']}")
                
                # Process each broker listing
                for listing_idx, listing_url in enumerate(broker_listings, 1):
                    print(f"Processing broker {listing_idx}/{len(broker_listings)} from {state['name']}")
                    
                    # Extract broker information
                    broker_info = self.extract_broker_info(listing_url)
                    
                    if broker_info:
                        # Add to data list
                        self.data.append(broker_info)
                        
                        # Add to processed URLs
                        self.processed_urls.add(listing_url)
                        
                        # Save progress every few brokers
                        if len(self.data) >= self.save_frequency:
                            self.save_progress()
                    
                    # Random delay between requests to avoid IP ban
                    time.sleep(random.uniform(2, 5))
                
                # Save any remaining data
                self.save_progress(force=True)
                
                # Random delay between states
                time.sleep(random.uniform(2, 5))
            
            print("\nScraping completed!")
            
        except Exception as e:
            print(f"Error running scraper: {str(e)}")
        finally:
            # Save any remaining data
            self.save_progress(force=True)
            
            # Close the WebDriver
            if self.driver:
                self.driver.quit()

if __name__ == "__main__":
    scraper = BusinessBrokerScraper(debug=True)
    scraper.run()
