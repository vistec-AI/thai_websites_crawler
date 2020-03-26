import argparse
from bs4 import BeautifulSoup
import requests
import json
import tqdm

def is_xml(url): return '.xml' in url

def get_urls(domain, timeout=(2, 5)):
    inp = f'{domain}/sitemap.xml' if not is_xml(domain) else domain
    if inp[-4:]!='.xml': return None
    with requests.get(inp, timeout=timeout) as r:
        urls = [i.text for i in BeautifulSoup(r.text,features='html.parser').find_all('loc')]
    main_urls = [url for url in urls if not is_xml(url)]
    sub_urls = sum([get_urls(url) for url in urls if is_xml(url)],[])
    return main_urls + sub_urls

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str)
    parser.add_argument('--output_path', type=str)
    args = parser.parse_args()
    
    if args.input_path.split('.')[-1]=='txt':
        with open(args.input_path,'r') as f: domains = f.readlines()
        domains = [f'https://www.{i.split()[1].lower()}' for i in domains]
    elif args.input_path.split('.')[-1]=='json':
        with open(args.input_path,'r') as f: domains = json.load(f)
    
    print(f'There are {len(domains)} domains')

    urls = []
    for i in tqdm.tqdm(range(len(domains))):
        print(domains[i])
        try:
            urls+=get_urls(domains[i])
        except:
            print(f'Failed at {domains[i]}')
        if i%100==0:
            print(f'Saving at {i}')
            with open(args.output_path, 'w') as f:
                json.dump(urls, f)
    with open(args.output_path, 'w') as f:
        json.dump(urls, f)
