import hatesonar
import pandas as pd
import json
import os
import re
import gensim
import numpy as np

class HateSpeechAnalyzer:
    """Class that loads text data and metadata into a pandas DataFrame. Currently
     only supports .json files. 'data' is the DataFrame intended to include the
     data that is to be processed. 'metadata' is the DataFrame that is including
     information about the data and is optional. Includes some natural language
     processing methods. Currently only a data cleaning step and a method that
     runs the HateSonar (https://github.com/Hironsan/HateSonar) module on
     specified data. The intention is to include more language processing
     methods such as TF-IDF.

     Example:

     import HateSpeechAnalyzer as hsa
     a = hsa.HateSpeechAnalyzer(data_directory_path)
     """



    def __init__(self, data_directory_path=None, metadata_directory_path=None):

        self.data = pd.DataFrame()
        self.metadata = pd.DataFrame()
        self.compiled_regex = None
        self.data_directory_path = data_directory_path
        self.metadata_directory_path = metadata_directory_path



    def load_json(self, data_directory_path=None,
                  metadata_directory_path=None):
        """Loads data and metadata files from their respective directory paths.
        Don't give path to file but to directory where the file(s) are kept.
        Will try to load all files in given directories.

        Example: a.load_json(data_dir) # If not given when initialized.

        """

        self.load_data_json(data_directory_path)
        self.load_metadata_json(metadata_directory_path)


        
    def load_data_json(self, data_directory_path=None):
        """Loads data files from given directory path. Don't give path to file
        but to directory where the file(s) are kept. Will try to load all files
        in given directory.

        Example: a.load_data_json(data_dir) # If not given when initialized.

        """

        print('Loading data into HateSpeechAnalyzer... \n')
        if self.data_directory_path is None:
            self.data_directory_path = data_directory_path
        else:
            data_directory_path = self.data_directory_path

        file_names = [f for f in os.listdir(data_directory_path) 
                      if os.path.isfile(os.path.join(data_directory_path, f))]
        dfs = list()
        for fn in file_names:
            path = data_directory_path + fn
            dfs.append( self.__load_data_json_file( path ) )

        self.data = pd.concat(dfs, ignore_index=True, copy=False)
        print('Data successfully loaded into HateSpeechAnalyzer. \n')
            


    def load_metadata_json(self, metadata_directory_path=None):
        """Loads metadata files from given directory path. Don't give path to
        file but to directory where the file(s) are kept. Will try to load all
        files in given directory.

        Example: a.load_metadata_json(metadata_dir) # If not given when initialized.

        """

        print('Loading metadata into HateSpeechAnalyzer... \n')
        if self.metadata_directory_path is None:
            self.metadata_directory_path = metadata_directory_path
        else:
            metadata_directory_path = self.metadata_directory_path

        file_names = [f for f in os.listdir(metadata_directory_path) 
                      if os.path.isfile(os.path.join(metadata_directory_path, f))]

        dfs = list()
        for fn in file_names:
            path = metadata_directory_path + fn
            dfs.append( self.__load_metadata_json_file( path ) )

        self.metadata = pd.concat(dfs, ignore_index=True, copy=False)
        print('Metadata successfully loaded into HateSpeechAnalyzer. \n')
        
            

    def clean_data(self, column, disallowed=None, remove_NaN=True,
                   remove_duplicates=True):

        """Removes data rows in given column in the data
        DataFrame. All parameters are optional and independent. Returns a
        dictionary with the record of removed items.

        column takes the name of the column as a string.

        disallowed takes a set of strings that should be removed:
            comment_list = {'deleted', 'don't want this comment'}

        remove_NaN takes a boolean.

        remove_duplicates takes a boolean.

        Example: removed_items = a.clean_data('body', disallowed=comment_list,
                                              regex=regex_input)
        """

        print('Cleaning: ', column, ', in data... \n')

        removed_items = {'Data_orig_shape' : self.data.shape,
                         'Data_new_shape' : 0,
                         'Removed_NaN' : 0,
                         'Removed_dups' : 0,
                         'Removed_disallowed' : 0}

        droplist = set() # Keeps the indices of rows to drop.

        self.data.sort_values(by=[column], inplace=True, ignore_index=True)
                                        
        for index, row in self.data.iterrows():

            if index in droplist: continue

            if remove_NaN: 
                i_to_drop = self.__remove_NaN(index, row, column)
                if i_to_drop:
                    removed_items['Removed_NaN'] += 1
                    droplist.add(i_to_drop)
                    continue

            if remove_duplicates:
                i_to_drop = self.__remove_duplicates(index, row, column)
                if i_to_drop:
                    removed_items['Removed_dups'] += len(i_to_drop)
                    droplist.update(i_to_drop)
                    continue
                                                            
            if disallowed is not None:
                i_to_drop = self.__remove_disallowed(index, row, column, disallowed)
                if i_to_drop:
                    removed_items['Removed_disallowed'] += 1
                    droplist.add(i_to_drop)
                    continue

        self.data.drop(droplist, inplace=True)
        self.data.reset_index(inplace=True, drop=True)
        removed_items['Data_new_shape'] = self.data.shape

        print('Cleaning of: ', column, ', done. \n')
        return removed_items



    def apply_regex(self, column, regex):
        """Applies specified regex to all rows in the specified column.

        regex takes a dictionary with form:
            {regex pattern: string to replace with}

        Example: regex_input = {r'http\S+': '_URL_ '}  # Replace a url with _URL_
        """

        print('Applying regex to: ', column, ' in data...')
        for index, row in self.data.iterrows():
            self.__apply_regex(index, row, column, regex)
        print('Regex applied to rows on: ', column, 'column.')



    def hate_sonar(self, column, already_lower=False):
        """Runs the HateSonar module (https://github.com/Hironsan/HateSonar) on
        the given column in the data DataFrame. Set already_lower to True if
        the text is in lower case already. Makes new columns in the data
        DataFrame where the results are added to.

        column takes the name of the column as a string.

        already_lower takes a boolean.

        Example: a.hate_sonar('body')
        """

        print('Running HateSonar on: ', column, ', in data... \n')
        sonar = hatesonar.Sonar()
        top_cls = list()
        hate_speech = list()
        offensive_lang = list()
        neither = list()

        for index, row in self.data.iterrows():

            if already_lower:
                sonar_result = sonar.ping(text = row[column])
            elif not already_lower:
                sonar_result = sonar.ping(text = row[column].lower())

            top_cls.append(sonar_result["top_class"])
            hate_speech.append(sonar_result["classes"][0]["confidence"])
            offensive_lang.append(sonar_result["classes"][1]["confidence"])
            neither.append(sonar_result["classes"][2]["confidence"])

        self.data["top_class"] = top_cls
        self.data["hate_speech"] = hate_speech
        self.data["offensive_language"] = offensive_lang
        self.data["neither"] = neither
        print('HateSonar on: ', column, ', finished. \n')


    def tf_idf(self, column):
        """Calculates Term frequency - Inverse document frequency and word count
        for all comments in the given column. Using the Gensim module. Returns
        two Pandas DataFrames, one holding the TF-IDF results and one word count.

        Example: freq_df, count_df = a.tf_idf('body')
        """

        # Join all strings to one long document
        document = [' '.join( self.data[column] )]

        # Convert document into a list of lowercase tokens, ignoring tokens
        # that are too short or too long.
        doc_tokenized = [gensim.utils.simple_preprocess(doc) for doc in document]

        dictionary = gensim.corpora.Dictionary()

        BoW_corpus = [dictionary.doc2bow(doc, allow_update=True) for
                      doc in doc_tokenized]

        tfidf = gensim.models.TfidfModel(BoW_corpus, smartirs='nfc')

        for doc in tfidf[BoW_corpus]:
            word_freq = [[dictionary[id], np.around(freq, decimals=3)] for id, freq in doc]

        for doc in BoW_corpus:
            word_count = [[dictionary[id], count] for id, count in doc]

        freq_df = pd.DataFrame(word_freq, columns=['Word', 'Frequency'])
        count_df = pd.DataFrame(word_count, columns=['Word', 'Count'])
        return freq_df, count_df



    def write_csv(self):
        """Writes both the data and the metadata to .csv files in the directory
        over the data directory respectively the metadata directory.

        Example: a.write_csv()
        """

        self.write_data_csv()
        self.write_metadata_csv()



    def write_data_csv(self):
        """Writes the data to a .csv file in the directory over the data
        directory.

        Example: a.write_data_csv()
        """

        path = self.data_directory_path[:-5]
        print('Writing data to: ', path + 'data.csv \n')
        self.data.to_csv(path + 'data.csv')



    def write_metadata_csv(self):
        """Writes the metadata to a .csv file in the directory over the metadata
        directory.

        Example: a.write_metadata_csv()"""

        path = self.metadata_directory_path[:-9]
        print('Writing metadata to: ', path + 'metadata.csv \n')
        self.metadata.to_csv(path + 'metadata.csv')


        
    def __remove_duplicates(self, index, row, column):                
        droplist = []
        end_of_df_index = self.data.shape[0] - 1
        dup_index = index + 1

        if dup_index > end_of_df_index: return False

        str1 = row[column]
        str2 = self.data.loc[dup_index].at[column]

        while (str1 == str2):
            droplist.append(dup_index)

            dup_index += 1

            if dup_index > end_of_df_index: return droplist

            str2 = self.data.loc[dup_index].at[column]
                        
        if droplist: return droplist

        return False        
        


    def __remove_NaN(self, index, row, column):
        if (pd.isna(row[column])) or (not row[column]):
            return index
        else:
            return False


        
    def __remove_disallowed(self, index, row, column, disallowed):
        if row[column] in disallowed:
            return index
        else:
            return False



    def __apply_regex(self, index, row, column, regex):
        new_string = row[column]
        for key, value in regex.items():
            new_string = re.sub(key, value, new_string)
        self.data.at[index, column] = new_string
        return None


    
    def __load_data_json_file(self, file_path):
        with open(file_path, 'r') as json_data:
            d = json.load(json_data)
            d = pd.json_normalize(d)
            return d
            


    def __load_metadata_json_file(self, file_path):
        with open(file_path, 'r') as json_data:
            md = json.load(json_data)
            md = pd.json_normalize(md)
            return md