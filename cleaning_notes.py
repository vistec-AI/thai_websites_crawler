import json
import multiprocessing
import tqdm
import pandas as pd
import dask.dataframe as dd
import tensorflow_hub as hub
import tensorflow_text
import tensorflow as tf
from sklearn.metrics.pairwise import cosine_similarity

#get the json as a result of `scrape_requests.py`
paracrawl_requests = json.load(open('data/paracrawl_requests.json'))
paracrawl_requests = sum(paracrawl_requests,[])

#break new lines
new_paracrawl = []
for k in paracrawl_requests:
    en_lst = k['en_text'].split('\n')
    th_lst = k['th_text'].split('\n')
    if len(en_lst)==len(th_lst):
        for i,j in zip(en_lst,th_lst):
            new_paracrawl.append({'en_text':i,'th_text':j})
    else:
        new_paracrawl.append({'en_text':k['en_text'],'th_text':k['th_text']})
        
#remove special tokens
paracrawl = pd.DataFrame(new_paracrawl).dropna().drop_duplicates().reset_index(drop=True)
paracrawl['en_text'] = paracrawl.en_text.map(lambda x: rm_useless_spaces(str(x).replace('\n',' ').replace('\t',' ').replace('\r','').strip()))
paracrawl['th_text'] = paracrawl.th_text.map(lambda x: rm_useless_spaces(str(x).replace('\n',' ').replace('\t',' ').replace('\r','').strip()))

#filter by percentage of characters and number of tokens
paracrawl['en_tokens'] = paracrawl.en_text.map(lambda x: len(x.split()))
def char_percent(pattern,text):
    return len(re.findall(pattern,text)) / (len(text)+0.01)
paracrawl['per_en'] = paracrawl.en_text.map(lambda x: char_percent(r'[a-zA-Z0-9]',x))
paracrawl['per_th'] = paracrawl.th_text.map(lambda x: char_percent(r'[ก-๙0-9]',x))
paracrawl['th_in_en'] = paracrawl.en_text.map(lambda x: 1 if char_percent(r'[ก-๙]',x) else 0)
paracrawl = paracrawl[paracrawl.th_in_en==0]
paracrawl = paracrawl[(paracrawl.en_tokens>5)&(paracrawl.en_tokens<150)&(paracrawl.per_en>0.5)&(paracrawl.per_th>0.5)]

#groupby to get unique en and th texts
paracrawl = paracrawl.groupby('en_text').max().sort_values('en_tokens').reset_index()
paracrawl = paracrawl.groupby('th_text').max().sort_values('en_tokens').reset_index()

#check parallel texts with universal sentence encoder
_emb_model = hub.load('https://tfhub.dev/google/universal-sentence-encoder-multilingual-large/3')
def use_similarity(sent1, sent2, emb_model = _emb_model):
    emb1, emb2 = emb_model(sent1),emb_model(sent2)
    return cosine_similarity(emb1,emb2)

bs = 1000
use_sim = []
for i in tqdm.tqdm(range(paracrawl.shape[0]//bs+1)):
    paracrawl_dd = dd.from_pandas(paracrawl[i*bs:(i+1)*bs], npartitions=multiprocessing.cpu_count() * 2)
    job = paracrawl_dd.apply(lambda row: use_similarity(row['en_text'],row['th_text'], _emb_model)[0,0], \
                             axis=1, meta=(None, 'float64'))
    use_sim += list(job.compute())
paracrawl['use_sim'] = use_sim
paracrawl = paracrawl[paracrawl.use_sim>0.5]

#save
paracrawl.to_csv('data/paracrawl_corpus.csv',index=False)
