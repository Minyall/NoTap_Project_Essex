import pandas as pd
import re
df = pd.read_pickle('processed_tweets.pkl')

url_re_pattern = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'

def text_cleaner(x):
    re.sub(url_re_pattern,'',x)
    split_text = x.split()
    split_text = [w for w in split_text if ('#' not in w) and (w.isalpha())]
    return ' '.join(split_text)

df['clean_text'] = df['text'].apply(text_cleaner)

df2 = df.drop_duplicates(subset='clean_text')

df2['duplicated'] = list(df.groupby(df['clean_text'],as_index=False).size())

df2.sort_values('duplicated',ascending=False).to_excel('tweet_duplication_scores.xls')
