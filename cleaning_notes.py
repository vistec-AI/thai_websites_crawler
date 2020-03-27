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
        
#clean
paracrawl = pd.DataFrame(new_paracrawl).dropna().drop_duplicates().reset_index(drop=True)
paracrawl['en_text'] = paracrawl.en_text.map(lambda x: rm_useless_spaces(str(x).replace('\n',' ').replace('\t',' ').replace('\r','').strip()))
paracrawl['th_text'] = paracrawl.th_text.map(lambda x: rm_useless_spaces(str(x).replace('\n',' ').replace('\t',' ').replace('\r','').strip()))
paracrawl['en_tokens'] = paracrawl.en_text.map(lambda x: len(x.split()))
def char_percent(pattern,text):
    return len(re.findall(pattern,text)) / (len(text)+0.01)
paracrawl['per_en'] = paracrawl.en_text.map(lambda x: char_percent(r'[a-zA-Z0-9]',x))
paracrawl['per_th'] = paracrawl.th_text.map(lambda x: char_percent(r'[ก-๙0-9]',x))
paracrawl['th_in_en'] = paracrawl.en_text.map(lambda x: 1 if char_percent(r'[ก-๙]',x) else 0)
paracrawl = paracrawl[paracrawl.th_in_en==0]
output = paracrawl[(paracrawl.en_tokens>5)&(paracrawl.en_tokens<150)&(paracrawl.per_en>0.5)&(paracrawl.per_th>0.5)]
output = output.groupby('en_text').max().sort_values('en_tokens').reset_index()
print(output.shape)
output.to_csv('data/paracrawl_corpus.csv',index=False)
