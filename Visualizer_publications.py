import pandas as pd
import matplotlib as mpl
mpl.use('Agg') #  Must be before pyplt is imported
from matplotlib import pyplot as plt

#  Import Data - Sampled and Full
sample_df = pd.read_pickle('retweets_extracted_processed_tweets.pkl')
full_df = pd.read_pickle('retweets_not_extracted.pkl')

#  Wrangle dates to create seperate month and year columns
sample_df['creation_date'] = pd.to_datetime(sample_df['creation_date'])
sample_df['year'] = sample_df['creation_date'].apply(lambda x: x.year)
sample_df['month'] = pd.to_datetime(sample_df['creation_date']).dt.to_period('M')
sample_df = sample_df[sample_df['year'] == 2017]

full_df['creation_date'] = pd.to_datetime(full_df['creation_date'])
full_df['year'] = full_df['creation_date'].apply(lambda x: x.year)
full_df['month'] = pd.to_datetime(full_df['creation_date']).dt.to_period('M')
full_df = full_df[full_df['year'] == 2017]

#  Classify each set based on whether it is full set or sampled, and join into one DataFrame
full_df['type'] = 'Full'
sample_df['type'] = 'Sample'
main_df = pd.concat([sample_df,full_df])

#  Plot using Pivot Table to count instances, divided by type -
# values is simply an object to count which will have no NaN values.

plt.style.use('fivethirtyeight')

ax = main_df.pivot_table(aggfunc='count', values=['tweet_id'],columns=['type'],index=['month']).plot.bar(width=0.9)
fig = ax.get_figure()
fig.set_size_inches(10,6)
for p in ax.patches:
    ax.annotate("%.0f" %p.get_height(), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center', va='center', xytext=(0, 10), textcoords='offset points')

ax.tick_params(bottom="off", top="off", left="off", right="off")
for key, val in ax.spines.items():
    ax.spines[key].set_visible(False)
ax.spines['bottom'].set_visible(True)
ax.grid(b=True, axis='y', color='gray')
ax.set_title('Frequency of Sampled #NoTap Tweets from 2017, by Month',y=1.08)
ax.set_xlabel('')
ax.set_ylabel('Frequency')
ax.set_axisbelow(b=True)
ax.set_xticklabels( ('March', 'April','May', 'June', 'July', 'August') )
fig.autofmt_xdate(bottom = 0.3)
ax.patch.set_facecolor('white')
ax.xaxis.grid(False)
ax.yaxis.grid(color='white')
legend = plt.legend()#bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0., frameon=1) UNCOMMENT TO MOVE LEGEND TO RIGHT
legend.get_texts()[0].set_text('Full Dataset')
legend.get_texts()[1].set_text('Sampled Dataset')
frame = legend.get_frame()
frame.set_color('white')
fig.set_facecolor('white')
fig.tight_layout()
mpl.rcParams['savefig.facecolor'] = 'white'


plt.savefig('FINAL - Full_and_Sampled_Bar_Plot.png', format='png', dpi=800)