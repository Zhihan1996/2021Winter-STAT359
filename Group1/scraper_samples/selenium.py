from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup


url = 'https://www.businesswire.com/portal/site/home/news/'

# config
options = Options()
options.headless = False

# initialize a webdriver
d = webdriver.Chrome(ChromeDriverManager().install())
d.get(url)

# use BeautifulSoup to parse the web
soup = BeautifulSoup(d.page_source, 'lxml')

all_urls = set()
for item in soup.find_all('meta', {'itemprop':'url'}):
    url = item['content']
    all_urls.add(url)

for i in range(2500):
    try:
        print(len(all_urls))
        d.find_element_by_css_selector('div[class*="pagingNext"]').click()
        soup = BeautifulSoup(d.page_source, 'lxml')
        for item in soup.find_all('meta', {'itemprop': 'url'}):
            url = item['content']
            if url not in last_urls:
                all_urls.add(url)
        time.sleep(2)
    except:
        time.sleep(2)
        continue
        last_height = d.execute_script("return document.body.scrollHeight")

        while True:
            # Scroll down to bottom
            d.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait to load page
            time.sleep(10)

            # Calculate new scroll height and compare with last scroll height
            new_height = d.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height