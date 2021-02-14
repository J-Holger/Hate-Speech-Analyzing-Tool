import requests
import json
import os


class RedditDataDL:
    """Class that uses the Pushshift API (https://github.com/pushshift/api) to
    retrieve data from Reddit forums. Read about the different search parameters
    that can be used on their site. This class requires a directory 'data' in the
    same place as the program.

    Example:
        import RedditDataDL as rddl
        d = rddl.RedditDataDL(endpoint='comment', before="1577923200",
                               after="1577836800",subreddit='climbharder')
    """



    def __init__(self, endpoint, q=None, ids=None, size=500, fields=None, sort=None,
                 sort_type=None, aggs=None, author=None, subreddit=None, after=None,
                 before=None, frequency=None, metadata='true'):

        # For the Pushshift URL
        self.endpoint = endpoint
        self.q = q
        self.ids = ids
        self.size = size
        self.fields = fields
        self.sort = sort
        self.sort_type = sort_type
        self.aggs = aggs
        self.author = author
        self.subreddit = subreddit
        self.after = after
        self.before = before
        self.frequency = frequency
        self.metadata = metadata

        # Class parameters
        self.request_counter = 0
        self.is_data_saved = False
        self.is_data_cleaned = False



    def get_data(self):
        """"Creates one sub directory in the 'data' directory. Then stores all
        downloaded data and metadata to it. Throws exception if the sub
        directory already exists.

        Example: d.get_data()
        """

        data_path = self.__make_path('data')
        metadata_path = self.__make_path('metadata')

        if os.path.exists(data_path) or os.path.exists(metadata_path):
            raise Exception('Directory to store either data or metadata ' +
                            '(or both) already exists. Remove or rename these' +
                            ' directories before downloading new data. ' +
                            'Directory paths:\n' + data_path + '\n' +
                            metadata_path)
        else:
            return self.__download_reddit_data(data_path, metadata_path)


    def get_paths(self):
        """Returns the paths to the directories that the data and metadata are
        going to be saved into."""

        data_path = self.__make_path('data')
        metadata_path = self.__make_path('metadata')
        return data_path, metadata_path



    def __url(self):
        """Creates a callable URL depending on what parameters given in the
        initialization."""

        if self.endpoint == 'comment':
            url = 'https://api.pushshift.io/reddit/search/' + self.endpoint + '/?'
            if self.q is not None: url += 'q=' + self.q +'&'
            if self.size is not None: url += 'size=' + str(self.size) + '&'
            if self.fields is not None: url += 'fields=' + self.fields + '&'
            if self.sort is not None: url += 'sort=' + self.sort + '&'
            if self.sort_type is not None: url += 'sort_type=' + self.sort_type + '&'
            if self.aggs is not None: url += 'aggs=' + self.aggs + '&'
            if self.author is not None: url += 'author=' + self.author + '&'
            if self.subreddit is not None: url += 'subreddit=' + self.subreddit + '&'
            if self.after is not None: url += 'after=' + str(self.after) + '&'
            if self.before is not None: url += 'before=' + str(self.before) + '&'
            if self.frequency is not None: url += 'frequency=' + self.frequency + '&'
            if self.metadata is not None: url += 'metadata=' + self.metadata + '&'
            if self.ids is not None: url = ('https://api.pushshift.io/reddit/' +
                                            'comment/search?ids=' + self.ids)
            return url

        elif self.endpoint == 'submission':
            url = 'https://api.pushshift.io/reddit/search/' + self.endpoint + '/?'
            if self.q is not None: url += 'q=' + self.q +'&'
            if self.size is not None: url += 'size=' + str(self.size) + '&'
            if self.sort is not None: url += 'sort=' + self.sort + '&'
            if self.sort_type is not None: url += 'sort_type=' + self.sort_type + '&'
            if self.aggs is not None: url += 'aggs=' + self.aggs + '&'
            if self.author is not None: url += 'author=' + self.author + '&'
            if self.subreddit is not None: url += 'subreddit=' + self.subreddit + '&'
            if self.after is not None: url += 'after=' + str(self.after) + '&'
            if self.before is not None: url += 'before=' + str(self.before) + '&'
            if self.frequency is not None: url += 'frequency=' + self.frequency + '&'
            if self.metadata is not None: url += 'metadata=' + self.metadata + '&'
            if self.ids is not None: url = ('https://api.pushshift.io/reddit/' +
                                            'submission/search?ids=' + self.ids)
            return url

        elif self.endpoint == 'comment_ids':
            """Not a working endpoint."""

            raise Exception('This endpoint is not supoprted in this version' +
                            ' the program.')

            url = 'https://api.pushshift.io/reddit/' + self.endpoint
            return url

        else:
            raise Exception('Invalid endpoint.')



    def __retrieve_reddit_data(self):
        """ Using the requests library to get the requested reddit data. 
        Then loads the reddit data with json library and decodes it to a string."""

        request_data = requests.get(self.__url())
        request_data.raise_for_status()
        retrieved_reddit_data = json.loads(request_data.text)
        return retrieved_reddit_data




    def __download_reddit_data(self, data_path, metadata_path):
        """Uses the retrieve_reddit_data method and saves data to a json file. Then changes the start time (after) 
        and reruns until all the data is collected. The json file has a first attribute "reddit_data" and its associated
        value is a list containing all the retrieved reddit data."""

        print('Started download from: ', self.subreddit, '. \n')
        reddit_data = self.__retrieve_reddit_data() # Use the class method to request reddit data and load it with json
        after_holder = self.after # Holder to keep track of the original after-input.

        data_file_name = ('data_' + str(self.request_counter) + '.json')
        metadata_file_name = ('metadata_' + str(self.request_counter) + '.json')
        data_directory = self.__make_directory(data_path)
        metadata_directory = self.__make_directory(metadata_path)

        while len(reddit_data['data']) > 0:
            with    open(data_directory + '\\' + data_file_name, 'w') as data_file, \
                    open(metadata_directory + '\\' + metadata_file_name, 'w') as metadata_file:

                    json.dump(reddit_data['data'], data_file, indent=4)
                    json.dump(reddit_data['metadata'], metadata_file, indent=4)
                    self.request_counter += 1

                    data_file_name = ('data_' + str(self.request_counter) + '.json')
                    metadata_file_name = ('metadata_' + str(self.request_counter) + '.json')

                    self.after = reddit_data['data'][-1]['created_utc']
                    reddit_data = self.__retrieve_reddit_data()
                    print(str(self.request_counter) + ' request(s) to reddit server done. \n')

        self.after = after_holder
        self.is_data_saved = True

        print('Download from: ', self.subreddit, ' finished. \n')
        return data_directory, metadata_directory



    def __make_path(self, sub_dir_name):
        """Creates the path to the new sub directory that are to be created."""

        current_working_directory = os.getcwd()
        new_directory = ('\\data\\' + self.subreddit + '_' + self.endpoint + '_' +
                         self.after + '_' + self.before + '\\' + sub_dir_name + '\\')

        path = current_working_directory + new_directory
        return path



    def __make_directory(self, path):
        """Creates a directory from th  given path."""

        os.makedirs(path)
        return path



    @classmethod
    def from_string(cls, url_string):
        """Alternative constructor to initialize from strin. Currently not
        working."""

        #return cls(q, ids, size, fields, sort, sort_type, aggs, author, subreddit, after, before, frequency, metadata)
        pass