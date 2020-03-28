#imports
import argparse
import re
from pythainlp.tokenize import sent_tokenize, word_tokenize
import pandas as pd
def char_percent(pattern,text):
    return len(re.findall(pattern,text)) / (len(text)+0.01)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str)
    args = parser.parse_args()

    #load scraped urls
    df = pd.read_csv(args.input_path)

    #missing
    df['missing_en'] = df.en_text.isna()
    df['missing_th'] = df.th_text.isna()

    #characters
    df['per_en'] = df.en_text.map(lambda x: char_percent(r'[a-zA-Z0-9]',str(x)))
    df['per_th'] = df.th_text.map(lambda x: char_percent(r'[ก-๙0-9]',str(x)))
    df['th_in_en'] = df.en_text.map(lambda x: 1 if char_percent(r'[ก-๙]',str(x)) else 0)

    #tokens
    df['en_tokens'] = df.en_text.map(lambda x: len(str(x).split()))
    df['th_tokens'] = df.th_text.map(lambda x: len(word_tokenize(str(x))))
    df['e2t_tokens'] = df.en_tokens / df.th_tokens

    #sentences
    df['en_sentences'] = df.en_text.map(lambda x: len(str(x).split('.')))
    df['th_sentences'] = df.th_text.map(lambda x: len(sent_tokenize(str(x))))
    
    print(f'''
    {args.input_path}
    shape: {df.shape}
    missing en: {df.missing_en.sum()} segments
    missing th: {df.missing_th.sum()} segments
    en duplicates: {df.en_text.count() - df.en_text.nunique()} segments
    th duplicates: {df.th_text.count() - df.th_text.nunique()} segments
    th charcters in en texts: {df.th_in_en.sum()} segments
    en char (mean, median, min, max): {df.per_en.mean():.2f}, {df.per_en.median():.2f} ({df.per_en.min():.2f}-{df.per_en.max():.2f})
    th char (mean, median, min, max): {df.per_th.mean():.2f}, {df.per_th.median():.2f} ({df.per_th.min():.2f}-{df.per_th.max():.2f})
    en tokens (mean, median, min, max): {df.en_tokens.mean():.2f}, {df.en_tokens.median()} ({df.en_tokens.min()}-{df.en_tokens.max()})
    th tokens (mean, median, min, max): {df.th_tokens.mean():.2f}, {df.th_tokens.median()} ({df.th_tokens.min()}-{df.th_tokens.max()})
    en-to-th tokens ratio (mean, median, min, max): {df.e2t_tokens.mean():.2f}, {df.e2t_tokens.median():.2f} ({df.e2t_tokens.min():.2f}-{df.e2t_tokens.max():.2f})
    en sentences (mean, median, min, max): {df.en_sentences.mean():.2f}, {df.en_sentences.median()} ({df.en_sentences.min()}-{df.en_sentences.max()})
    th sentences (mean, median, min, max): {df.th_sentences.mean():.2f}, {df.th_sentences.median()} ({df.th_sentences.min()}-{df.th_sentences.max()})
    ''')
