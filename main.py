import HateSpeechAnalyzer as hsa
import RedditDataDL as rddl
import pandas as pd

# Initialize RedditDataDL with parameters according to https://github.com/pushshift/api
d = rddl.RedditDataDL(endpoint='comment', before="1612134000",
                               after="1420066800",subreddit='TheRedPill')

# Download the Reddit data.
data_dir, metadata_dir = d.get_data()



# Initialize HateSpeechAnalyzer.
a = hsa.HateSpeechAnalyzer()


# Load the Reddit data that was downloaded into the HateSpeechAnalyzer.
a.load_json(data_dir, metadata_dir)


# Print the shapes of the data
print('Data has {} rows and {} columns'.format(a.data.shape[0], a.data.shape[1]))


# Define a list of comments that you want removed.
comment_list = {'[removed]', '[deleted]', '_URL_'}


# Define the regex expressions.
regex_input = {r'@\w+' : '@USER ', # Replace a mention by @ with @User
               r'http\S+' : '_URL_ ', # Replace a url with _URL_
               r'\s+' : ' ', # Removes extra spaces to only one
               r'[^!"%-&\'(),./:;?_`A-Za-z0-9\s]' : '', # Removes all char except in the list
               r'&gt;\s' : '', # Removes &gt; with a space after
               r'&gt;' : '', # Removes &gt; without a space after
               r'&amp;x200B;' : '', # Removes &amp;x200B;
               r'^\s+|\s+$' : '', # Remove leading and trailing spaces
               r'^\W+' : ''}


# Clean data in the given column and store the record.
a.apply_regex('body', regex_input)
removed_items = a.clean_data('body', disallowed=comment_list)


# Write the removed items DF to .csv
removed_items.to_csv(data_dir[:-5] + 'removed_items.csv')


# Run the data in given column through the HateSonar module (https://github.com/Hironsan/HateSonar)
# The new data created is saved in the correct row in the a.data DataFrame.
a.hate_sonar('body')


# Calculate TF-IDF on the whole dataset using gensim and then write resulting
# DataFrames to .csv.
freq_df, count_df = a.tf_idf('body')
freq_df.to_csv(data_dir[:-5] + 'tf_idf.csv')
count_df.to_csv(data_dir[:-5] + 'word_count.csv')


# Write data and metadata to .csv files.
a.write_csv()