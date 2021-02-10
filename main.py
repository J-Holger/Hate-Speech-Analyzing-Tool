import HateSpeechAnalyzer as hsa
import RedditDataDL as rddl
import pandas as pd

# Initialize RedditDataDL with parameters according to https://github.com/pushshift/api
#d = rddl.RedditDataDL(endpoint='comment', before="1609459200",
#                               after="1577836800",subreddit='FemaleDatingStrategy')

# Download the Reddit data. Default is 500 posts per call.
#data_dir, metadata_dir = d.get_data()
data_dir = 'C:/Users/holge/my_projects/HateSpeechAnalyzingTool/data/TheRedPill_comment_1577836800_1609459200/data/'
metadata_dir = 'C:/Users/holge/my_projects/HateSpeechAnalyzingTool/data/TheRedPill_comment_1577836800_1609459200/metadata/'


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

# Make a DataFrame of the record so it's easy to handle.
removed_items = pd.DataFrame(data=removed_items)

# Run the data in given column through the HateSonar module (https://github.com/Hironsan/HateSonar)
# The new data created is saved in the correct row in the a.data DataFrame.
a.hate_sonar('body')

# Write everything to .csv files.
a.write_csv()
removed_items.to_csv(data_dir[:-5] + 'removed_items.csv') # Using the pandas method.