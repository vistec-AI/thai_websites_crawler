from bs4 import BeautifulSoup
import requests
import tqdm
import re
import json
import argparse

def sitemaps_from_robots(domain,max_len=200):
    with requests.get(f'{domain}/robots.txt') as r:
        xmls = re.findall(r'https://.+?\.xml', r.text)
    return [xml for xml in xmls if len(xml)<=max_len]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str)
    parser.add_argument('--output_path', type=str)
    parser.add_argument('--max_len', type=int, default=200)
    args = parser.parse_args()
    
    with open(args.input_path,'r') as f: 
        domains = f.readlines()

    domains = [f'https://www.{i.split()[1].lower()}' for i in domains]
    print(f'There are {len(domains)} domains')

    sitemaps = []
    for i in tqdm.tqdm(range(len(domains))):
        try:
            sitemaps+=sitemaps_from_robots(domains[i], max_len=args.max_len)
        except:
            print(f'Failed at {domains[i]}')
        if i%100==0:
            print(f'Saving at {i}')
            with open(f'{args.output_path}', 'w') as f:
                json.dump(sitemaps, f)
    with open(f'{args.output_path}', 'w') as f:
        json.dump(sitemaps, f)
        
#     #optional for deduplicate with all_urls.json
#     robots_urls = set(json.load(open('data/robots_urls.json','r')))
#     all_urls = set(json.load(open('data/all_urls.json','r')))
#     diff_urls = list(robots_urls - all_urls)
#     with open('data/diff_urls.json', 'w') as f:
#         json.dump(diff_urls, f)
    