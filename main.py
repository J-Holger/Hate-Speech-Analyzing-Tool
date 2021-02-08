import HateSpeechAnalyzer as hsa
import RedditDataDL as rddl
import pandas as pd

# Initialize RedditDataDL with parameters according to https://github.com/pushshift/api
d = rddl.RedditDataDL(endpoint='comment', before="1609459200",
                               after="1577836800",subreddit='FemaleDatingStrategy')

# Download the Reddit data. Default is 500 posts per call.
data_dir, metadata_dir = d.get_data()



# Initialize HateSpeechAnalyzer.
a = hsa.HateSpeechAnalyzer()

# Load the Reddit data that was downloaded into the HateSpeechAnalyzer.
a.load_json(data_dir, metadata_dir)

# Check that the data looks good
print( a.data.head() )
print( a.metadata.head() )
print(a.data.shape)

# Define a list of comments that you want removed.
comment_list = {'[removed]', 'Your submission to /r/TheRedPill has been removed. **[DO NOT CONTACT MODS, WE WILL NOT REVERSE THIS DECISION](https://www.reddit.com/r/TheRedPill/comments/80kgg6/shit_to_avoid_saying_in_modmail/)** You have a new account with little karma. Please lurk and contribute more. **READ THE SIDEBAR**.\n\n*I am a bot, and this action was performed automatically. Please [contact the moderators of this subreddit](/message/compose/?to=/r/TheRedPill) if you have any questions or concerns.*',
               '[deleted]', '_URL_'}

# Define the regex expressions.
regex_input = {r'@\w+' : '@USER ', # Replace a mention by @ with @User
              r'http\S+' : '_URL_ ', # Replace a url with _URL_
              r'\s+' : ' ', # Removes extra spaces to only one
              r'[^!"%-&\'(),./:;?_`A-Za-z0-9\s]' : '', # Removes all char except in the list
              r'&gt;\s' : '', # Removes &gt; with a space after
              r'&gt;' : '', # Removes &gt; without a space after
              r'&amp;x200B;' : '', # Removes &amp;x200B;
              }

# Clean data in the given column and store the record.
removed_items = a.clean_data('body', disallowed=comment_list, regex=regex_input)

# Make a DataFrame of the record so it's easy to handle.
removed_items = pd.DataFrame(data=removed_items)

# Run the data in given column through the HateSonar module (https://github.com/Hironsan/HateSonar)
# The new data created is saved in the correct row in the a.data DataFrame.
a.hate_sonar('body')

# Write everything to .csv files.
a.write_csv()
removed_items.to_csv(data_dir[:-5] + 'removed_items.csv') # Using the pandas method.