import hatesonar
import pandas as pd
import json
import os
import re

class HateSpeechAnalyser:

    def __init__(self, data_directory_path=None, metadata_directory_path=None):
        self.data = pd.DataFrame()
        self.metadata = pd.DataFrame()
        self.compiled_regex = None
        self.data_directory_path = data_directory_path
        self.metadata_directory_path = metadata_directory_path


    def load_json(self, data_directory_path=None,
                  metadata_directory_path=None):

        self.load_data_json(data_directory_path)
        self.load_metadata_json(metadata_directory_path)
        
        
    def load_data_json(self, data_directory_path=None):
        if self.data_directory_path is None:
            self.data_directory_path = data_directory_path
        else:
            data_directory_path = self.data_directory_path

        file_names = [f for f in os.listdir(data_directory_path) 
                      if os.path.isfile(os.path.join(data_directory_path, f))]   
        for fn in file_names:
            self.__load_data_json_file(data_directory_path + fn)
            

    def load_metadata_json(self, metadata_directory_path=None):
        if self.metadata_directory_path is None:
            self.metadata_directory_path = metadata_directory_path
        else:
            metadata_directory_path = self.metadata_directory_path

        file_names = [f for f in os.listdir(metadata_directory_path) 
                      if os.path.isfile(os.path.join(metadata_directory_path, f))]    
        for fn in file_names:
            self.__load_data_json_file(metadata_directory_path + fn)      
        
            
            
    def clean_data(self, column, disallowed=None, regex=None, remove_NaN=True, 
                          remove_duplicates=True):
        removed_items = {'Data_orig_shape' : self.data.shape,
                         'Data_new_shape' : 0,
                         'Removed_NaN' : 0,
                         'Removed_dups' : 0,
                         'Removed_disallowed' : 0}
        droplist = set()
        
        """I want a function that only traverse the the rows in the dataframe once. It should
        first do a sort before starting the traverse. Then check for duplicates and if the row 
        is a duplicate then add to droplist. If something is added to the droplist during an iteration
        The other filters should be skipped."""
        
        self.data.sort_values(by=[column], inplace=True, ignore_index=True)
                                        
        for index, row in self.data.iterrows():
            
            if index in droplist: continue
            
            if remove_duplicates:
                i_to_drop = self.__remove_duplicates(index, row, column)
                if i_to_drop:
                    removed_items['Removed_dups'] += len(i_to_drop)
                    droplist.update(i_to_drop)
                    continue
                    
            if remove_NaN: 
                i_to_drop = self.__remove_NaN(index, row, column)
                if i_to_drop:
                    removed_items['Removed_NaN'] += 1
                    droplist.add(i_to_drop)
                    continue
                                                            
            if disallowed is not None:
                i_to_drop = self.__remove_disallowed(index, row, column, disallowed)
                if i_to_drop:
                    removed_items['Removed_disallowed'] += 1
                    droplist.add(i_to_drop)
                    continue
                
            if regex is not None:
                self.__apply_regex(index, row, column, regex)
        
        self.data.drop(droplist, inplace=True)
        self.data.reset_index(inplace=True, drop=True)
        removed_items['Data_new_shape'] = self.data.shape
        return removed_items
            
        
    def __remove_duplicates(self, index, row, column):                
        droplist = []
        end_of_df = self.data.shape[0]
        dup_index = index + 1
        if dup_index == end_of_df: return False
        str1 = row[column]
        str2 = self.data.loc[dup_index].at[column]
        while ( (dup_index < end_of_df) and (str1 == str2) ):
            droplist.append(dup_index)
            dup_index += 1
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
            
                                                                                     
    def __load_json_file(self, data_file_path, metadata_file_path):
        print("__load_json_file called but doesn't do anything.")
     #   self.__load_data_json(data_file_path)
      #  self.__load_metadata_json(metadata_file_path)
    
    def __load_data_json_file(self, file_path):
        print(file_path)
        with open(file_path, 'r') as json_data:
            d = json.load(json_data)
            d = pd.json_normalize(d)
            self.data = self.data.append(d, ignore_index=True)
            
            
    def __load_metadata_json_file(self, file_path):
        with open(file_path, 'r') as json_data:
            md = json.load(json_data)
            md = pd.json_normalize(md)
            self.metadata = self.metadata.append(md, ignore_index=True)
    
    
    def hate_sonar(self, column, already_lower=False):
        
        sonar = hatesonar.Sonar()
        top_cls = list()
        hate_speech = list()
        offensive_lang = list()
        neither = list()
        for index, row in self.data.iterrows():
            if already_lower: 
                sonar_result = sonar.ping( text=row[column] )
            elif not already_lower:
                sonar_result = sonar.ping( text=row[column].lower() )
            top_cls.append(sonar_result["top_class"])
            hate_speech.append(sonar_result["classes"][0]["confidence"])
            offensive_lang.append(sonar_result["classes"][1]["confidence"])
            neither.append(sonar_result["classes"][2]["confidence"])
        
        self.data["top_class"] = top_cls
        self.data["hate_speech"] = hate_speech
        self.data["offensive_language"] = offensive_lang
        self.data["neither"] = neither
        
        
        
    def write_csv(self):
        self.write_data_csv()
        self.write_metadata_csv()

    def write_data_csv(self):
        path = self.data_directory_path[:-5]
        print('Path to write csv to is: ', path + 'data.csv')
        self.data.to_csv(path + 'data.csv')

    def write_metadata_csv(self):
        path = self.metadata_directory_path[:-9]
        print('Path to write csv to is: ', path + 'metadata.csv')
        self.metadata.to_csv(path + 'metadata.csv')

            
            

