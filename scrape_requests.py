import argparse
from bs4 import BeautifulSoup
import requests
import json
import tqdm
import multiprocessing
from pythainlp.ulmfit import rm_useless_spaces

def get_parallel_texts(parallel_url, timeout=(10,10), tags = ['h1','h2','h3','h4','h5','h6','p']):
    try:
        with requests.get(parallel_url['en_url'],timeout=timeout) as r:
            soup_en = BeautifulSoup(r.content,features='html.parser')
        with requests.get(parallel_url['th_url'],timeout=timeout) as r:
            soup_th = BeautifulSoup(r.content,features='html.parser')
    except:
        print('Request error')
        return None
          
    parallel_texts = []
    for tag in tags:
        tags_en = soup_en.find_all(tag)
        tags_th = soup_th.find_all(tag)

        if len(tags_en)!=len(tags_th):
#             print(f'{tag} tags not paired. Skipping.')
            continue
        elif (len(tags_en)==0)|(len(tags_th)==0):
#             print(f'{tag} tags do not exist. Skipping.')
            continue
        else:
            for tag_en, tag_th in zip(tags_en, tags_th):
                parallel_texts.append({'en_text': rm_useless_spaces(tag_en.get_text(separator=" ")),
                                       'th_text': rm_useless_spaces(tag_th.get_text(separator=" "))})
    return parallel_texts

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str)
    parser.add_argument('--output_path', type=str)
    parser.add_argument('--bs', type=int, default=multiprocessing.cpu_count() * 50)
    args = parser.parse_args()
    
    with open(args.input_path,'r') as f: parallel_urls = json.load(f)
        
    print(f'There are {len(parallel_urls)} parallel urls')

    parallel_texts = []
    total_batches = len(parallel_urls)//args.bs+1
    save_every = total_batches//10
    for i in tqdm.tqdm(range(total_batches)):
        parallel_urls_subs = parallel_urls[i*args.bs:(i+1)*args.bs]
        p = multiprocessing.Pool(multiprocessing.cpu_count())
        res = p.map(get_parallel_texts, parallel_urls_subs)
        parallel_texts+=[r for r in res if r is not None]
        p.terminate()
        p.join()
        if i%save_every==0:
            with open(args.output_path, 'w') as f:
                json.dump(parallel_texts, f)
           
    #save
    with open(args.output_path, 'w') as f:
        json.dump(parallel_texts, f)
