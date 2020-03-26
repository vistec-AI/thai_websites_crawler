import requests
import json
import re
import tqdm
import multiprocessing
from functools import partial
import argparse

def get_parallel_url(from_url, lang_from='th', lang_to='en'):
    d = {f'{lang_from}_url':'',f'{lang_to}_url':''}
    to_url = from_url.replace(f'/{lang_from}/',f'/{lang_to}/')
    blank_url = from_url.replace(f'/{lang_from}/','/')
    
    try:
        #from
        with requests.head(from_url) as r:
            if (r.status_code==200)&('404' not in str(r.url)):
                d[f'{lang_from}_url']  = from_url
            else:
                return None
        #to
        with requests.head(to_url) as r:
            if (r.status_code==200)&('404' not in str(r.url)):
                d[f'{lang_to}_url']  = to_url
            else:
                pass
        #blank
        if d[f'{lang_to}_url']=='':
            with requests.head(blank_url) as r:
                if (r.status_code==200)&('404' not in str(r.url)):
                    d[f'{lang_to}_url']  = blank_url
                else:
                    return None
    except:
        print('Request error')
        return None
    return d

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str)
    parser.add_argument('--lang1', type=str)
    parser.add_argument('--lang2', type=str)
    parser.add_argument('--output_path', type=str)
    parser.add_argument('--bs', type=int, default=multiprocessing.cpu_count() * 50)
    args = parser.parse_args()
   
    print(f'Parallelizing with {args.bs} processes')

    #load scraped urls
    with open(args.input_path,'r') as f: all_urls = json.load(f)

    #start finding those with language codes
    lang1_urls = []
    lang2_urls = []
    other_urls = []
    lang_pattern = r'/(\w{2})/'

    for url in tqdm.tqdm(all_urls):
        lang = re.search(lang_pattern,url)
        lang = lang.group() if lang is not None else ''
        if lang == f'/{args.lang1}/':
            lang1_urls.append(url)
        elif lang == f'/{args.lang2}/':
            lang2_urls.append(url)
        elif len(lang)==4:
            other_urls.append(url)

    print(
    f'''
    There are:
    {args.lang1}: {len(lang1_urls)} urls
    {args.lang2}: {len(lang2_urls)} urls
    others: {len(other_urls)} urls
    ''')

    #remove overlaps
    lang2_to_lang1 = []
    for lang2_url in lang2_urls: lang2_to_lang1.append(lang2_url.replace(f'/{args.lang2}/',f'/{args.lang1}/'))
    lang2_urls_only = list(set(lang2_to_lang1) - set(lang1_urls))
    lang2_urls_filt = []
    for lang2_url in lang2_urls_only: lang2_urls_filt.append(lang2_url.replace(f'/{args.lang1}/',f'/{args.lang2}/'))

    print(
    f'''
    There are:
    {args.lang1}: {len(lang1_urls)} urls
    {args.lang2}: {len(lang2_urls_filt)} filtered urls
    others: {len(other_urls)} urls
    ''')

    get parallel lang1
    parallel1 = []
    for i in tqdm.tqdm(range(len(lang1_urls)//args.bs+1)):
        lang1_subs = lang1_urls[i*args.bs:(i+1)*args.bs]
        p = multiprocessing.Pool(multiprocessing.cpu_count())
        res = p.map(partial(get_parallel_url,lang_from=args.lang1,lang_to=args.lang2), lang1_subs)
        parallel1+=[r for r in res if r is not None] 
        p.terminate()
        p.join()
    print(f'There are {len(parallel1)} pairs of parallel urls in {args.lang1}')

    #get parallel lang2
    parallel2 = []
    for i in tqdm.tqdm(range(len(lang2_urls_filt)//args.bs+1)):
        lang2_subs = lang2_urls[i*args.bs:(i+1)*args.bs]
        p = multiprocessing.Pool(multiprocessing.cpu_count())
        res = p.map(partial(get_parallel_url,lang_from=args.lang2,lang_to=args.lang1), lang2_subs)
        parallel2+=[r for r in res if r is not None]
        p.terminate()
        p.join()
    print(f'There are {len(parallel2)} pairs of parallel urls in {args.lang2}')

    #save
    with open(args.output_path, 'w') as f:
        json.dump(parallel1+parallel2, f)
