import argparse
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys


USERNAME = "YOUR_USERNAME"
PASSWORD = "YOUR_PASSWORD"

def get_login_page_url():
    return "https://www.alexa.com/login"

def get_ranking_table_url(page_number:int = 1):
    return f"https://www.alexa.com/topsites/countries;{page_number}/TH"


if __name__ == "__main__":

    driver = webdriver.Chrome(executable_path="/path/to/chromedriver_78")

    target_url = get_login_page_url()
    driver.get(target_url)

    username_element = driver.find_element_by_id("email")
    username_element.send_keys(USERNAME)

    password_element = driver.find_element_by_id("pwd")
    password_element.send_keys(PASSWORD)
    password_element.send_keys(Keys.RETURN)

    domains_list = []
    rank = 1
    print('Move to ranking pages')
    for page_number in range(0,20,1): # 0 - 19

        time.sleep(1)
        target_url = get_ranking_table_url(page_number)
        driver.get(target_url)
        
        time.sleep(5)

        detail_rows = driver.find_elements_by_class_name("site-listing")
        print('\nPage number: {}'.format(page_number + 1))
        print('Info: Number of rows = {}'.format(len(detail_rows)))

        for row in detail_rows:
            
            domain_column = row.find_element_by_class_name("DescriptionCell") # second columns

            domain = domain_column.text
            print(f"rank: {rank}, domain: {domain}")
            domains_list.append((rank, domain))

            rank+=1
        driver.get(target_url)

    driver.close()

    output_path = "./data/thai_domain_alexa.txt"
    print(f'Writing the result to {output_path}')
    
    with open(output_path, 'w', encoding="utf-8") as f:
        for (rank, domain) in domains_list:
            f.write(f'{rank}, {domain}\n')
    print('Done.')