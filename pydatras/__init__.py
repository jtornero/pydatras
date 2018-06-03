# pydatras.py: A Python library for downloading ICES/CIEM DATRAS datasets
# Copyright (C) 2018  Jorge Tornero, Instituto Español de Oceanografía
# jorge.tornero[at]ieo.es

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>


from __future__ import print_function
from zeep.client import Client
from zeep.helpers import serialize_object
from itertools import product
import pandas as pd


class DatrasHHData(pd.DataFrame):
    """ 
    A subclass of pandas Dataframe, just to make easy
    checking if the data comes from a DATRAS HL dataset
    """
        
    def __init__(self, *args, **kwargs):
        super(DatrasHHData, self).__init__(*args, **kwargs)
        
class DatrasHLData(pd.DataFrame):
    """
    A subclass of pandas Dataframe, just to make easy to
    checking if the data comes from a DATRAS HL dataset
    """
    def __init__(self, *args, **kwargs):
        super(DatrasHLData, self).__init__(*args, **kwargs)

class DATRASClient(object):
    """
    Class for a ICES/CIEM DATRAS Web Services datasets downloader.
    The class returns the datasets as pandas Dataframe (or, in certain cases,
    subclasses of them with the sole purpose of easing data checks in the future)
    
    Multiple dataset download can be achieved providing the proper arguments to the
    methods call. When passing lists of years, quarters, surveys, etc. as parameters,
    the methods create all possible combinations between them, but the final product
    contains only those combinations with data inside DATRAS. Because downloads of
    certain datasets (mostly HL datasets) are bulky and time-consuming, the number
    of datasets downloaded at once is limited, despite is possible to modify the
    limits or override them.
    """
    def __init__(
                 self,\
                 datras_url='https://datras.ices.dk/WebServices/DATRASWebService.asmx?WSDL',\
                 worms_url='http://www.marinespecies.org/aphia.php?p=soap&wsdl=1',\
                 download_limits=5):
        """
        Initializes the DATRASClient object
        Parameters:
            datras_url: string with the URL of the ICES/CIEM DATRAS WebService
            worms_url: string with the URL of the WORMS WebService
            download_limits: Integer which limits the maximum number of datasets
                to be downloaded from DATRAS. Download of HH and specially HL data
                could be time-consuming so it must be limited. However, this can be
                overriden in data download function call.
        """
        self.download_limits =  download_limits
        
        self.setDatrasClient(datras_url)
        
        self.setWormsClient(worms_url)
        
    def setDatrasClient(self, url):
        """
        Creates a ICES/CIEM DATRAS WebServices client with the provided url.
        Parameters:
            url: string with the URL of the ICES/CIEM DATRAS WebService
        """
        try:
            self.datras_client = Client(url)
        except:
            self.datras_client = None
    
    def setWormsClient(self, url):
        """
        Creates a WORMS WebServices client with the provided url.
        Parameters:
            url: string with the URL of the WORMS WebService
        """
        try:
            self.worms_client = Client(url)
        except:
            self.worms_client = None
    
    def getHHdata(self, survey, year, quarter, limit_download = True):
        """
        Returns a dataframe of haul information of the specified surveys, years and quarters.
        Multiple datasets can be downloaded at once, though its total number is limited by the
        parameter download_limits of the class constructor. However, this behavior can be
        overriden with the limit_download parameter in the function call.
        Parameters:
            survey: one or many (as a list) survey names (SP-ARSA, IBTS...)
            year: one or many (as a list)  years as integers (2010, 2011...)
            quarter: one or many (as a list) of quarters (1, 2...)
            limit_download: Boolean indicating whether download limits should be observed or not.
        Examples:
            haul_data = pydatras.getHHData(['SP-ARSA', 'SP-NORTH'], 2010, [1, 2])
            haul_data = pydatras.getHHData(['SP-ARSA'], list(range(2010, 2019)), 1, limit_download = False)
        """
        
        if self.datras_client is None:
            print ('DATRAS client not set')
            return
        
        if not isinstance(survey, list):
            survey = [survey]
        
        if not isinstance(year, list):
            year = [year]
            
        if not isinstance(quarter, list):
            quarter = [quarter]
            
        datasets = [ params for params in product(survey, year, quarter)]
                
        datasets_number = len(datasets)
        
        if limit_download and (datasets_number > self.download_limits):
            print("Data download is limited to %i datasets." % self.download_limits)
            print("Your are trying to download %i datasets. Exiting" % datasets_number)
            return
        
        downloaded_data = DatrasHHData()
        
        downloaded = 0
        
        for dataset in datasets:
            try:
                data = self.datras_client.service.getHHdata(dataset[0], dataset[1], dataset[2])
                if data is not None:
                    downloaded_data = downloaded_data.append(serialize_object(data))
                    downloaded += 1
            except:
                pass
        
        # Cleaning the dataframe
        
        string_fields = downloaded_data.select_dtypes(['object'])
        
        downloaded_data[string_fields.columns] = string_fields.apply(lambda x: x.str.strip())
        
        downloaded_data.reset_index()
        
        print("%i out of %i Datasets downloaded" %(downloaded, datasets_number))
        return downloaded_data

    def getHLdata(self, survey, year, quarter, translate_sps=False, limit_download = True):
        """
        Returns a dataframe whith length frequency distributions information of the
        specified surveys, years and quarters.
        Multiple datasets can be downloaded at once, though its total number is limited by the
        parameter download_limits of the class constructor. However, this behavior can be
        overriden with the limit_download parameter in the function call.
        Parameters:
            survey: one or many (as a list) survey names (SP-ARSA, IBTS...)
            year: one or many (as a list)  years as integers (2010, 2011...)
            quarter: one or many (as a list) of quarters (1, 2...)
            limit_download: Boolean indicating whether download limits should be observed or not.
                WARNING: Downloading HL data is time consuming. Use carefully.
        Examples:
            lfd_data = pydatras.getHLdata(['SP-ARSA', 'SP-NORTH'], 2010, [1, 2])
            lfd_data = pydatras.getHLdata(['SP-ARSA'], list(range(2010, 2019)), 1, limit_download = False)
        """
        if self.datras_client is None:
            print ('DATRAS client not set')
            return
        
        if not isinstance(survey, list):
            survey = [survey]
        
        if not isinstance(year, list):
            year = [year]
            
        if not isinstance(quarter, list):
            quarter = [quarter]
            
        datasets = [ params for params in product(survey, year, quarter)]
                
        datasets_number = len(datasets)
        
        if limit_download and (datasets_number > self.download_limits):
            print("Data download is limited to %i datasets." % self.download_limits)
            print("Your are trying to download %i datasets. Exiting" % datasets_number)
            return
        
        downloaded_data = DatrasHLData()
        
        downloaded = 0
        
        for dataset in datasets:
            try:
                data = self.datras_client.service.getHLdata(dataset[0], dataset[1], dataset[2])
                if data is not None:
                    downloaded_data = downloaded_data.append(serialize_object(data))
                    downloaded += 1
            except:
                pass
        
        print("%i out of %i Datasets downloaded" %(downloaded, datasets_number))
        
        if translate_sps:
            if self.worms_client is None:
                print("WORMS web service not available. Cannot resolve Aphia codes into species")
            else:
                codes = downloaded_data.Valid_Aphia.unique()
                print(codes)
                names = pd.DataFrame()
                for code in codes:
                    try:
                        name =  self.worms_client.service.getAphiaNameByID(code)
                    
                    except:
                        name = 'No data'
                    
                    print (code,name)
                    names = names.append({'Valid_Aphia':code,'Species_Name':name}, ignore_index = True)
                
                downloaded_data = pd.merge(downloaded_data, names, on='Valid_Aphia')
                
        # Cleaning the dataframe
        
        string_fields = downloaded_data.select_dtypes(['object'])
        
        downloaded_data[string_fields.columns] = string_fields.apply(lambda x: x.str.strip())
        
        downloaded_data.reset_index()
        
        return downloaded_data

    def getCAdata(self, survey, year, quarter, limit_download = True):
        """
        Returns a dataframewith age-based of the specified surveys, years and quarters.
        Multiple datasets can be downloaded at once, though its total number is limited by the
        parameter download_limits of the class constructor. However, this behavior can be
        overriden with the limit_download parameter in the function call.
        Parameters:
            survey: one or many (as a list) survey names (SP-ARSA, IBTS...)
            year: one or many (as a list)  years as integers (2010, 2011...)
            quarter: one or many (as a list) of quarters (1, 2...)
            limit_download: Boolean indicating whether download limits should be observed or not.
        Examples:
            age_data = pydatras.getCAdata(['SP-ARSA', 'SP-NORTH'],2010,[1, 2])
            age_data = pydatras.getCAdata(['SP-ARSA'],list(range(2010,2019)),1, limit_download = False)
        """
        print("getCAdata not yet implemented")
   

    def getSurveyInsertDate(self, survey, year, quarter, ship, country, limit_download = True):
        """
        Returns a dataframe of haul information of the specified surveys, years and quarters.
        Multiple datasets can be downloaded at once, though its total number is limited by the
        parameter download_limits of the class constructor. However, this behavior can be
        overriden with the limit_download parameter in the function call.
        Parameters:
            survey: one or many (as a list) survey names (SP-ARSA, IBTS...)
            year: one or many (as a list)  years as integers (2010, 2011...)
            quarter: one or many (as a list) of quarters (1, 2...)
            ship: one or many (as a list) of ships (DEN2, CSA...)
            country: one or many (as a list) of countries (ESP, DAN)
            limit_download: Boolean indicating whether download limits should be observed or not.
        Examples:
            date_data = pydatras.getSurveyInsertDate(['SP-ARSA', 'SP-NORTH'], 2010, 1, 'DAN2', 'GER')
            date_data = pydatras.getSurveyInsertDate(['SP-ARSA'],list(range(2010,2019)), 1, 'VZA', 'ESP', 'limit_download = False)
        """
        print("getSurveyInsertDate not yet implemented")

    def getSurveyList(self):
        """
        Returns a dataframe with the names of the surveys in DATRAS
        Example:
            df = pydatras.getSurveyList()
        """
        if self.datras_client is None:
            print ('DATRAS client not set')
            return
        
        try:
            data = serialize_object(self.datras_client.service.getSurveyList())
            downloaded_data = pd.DataFrame(data)
        
        except:
            print('Cannot download survey list')
            return
        
        # Cleaning the dataframe
      
        string_fields = downloaded_data.select_dtypes(['object'])
        
        downloaded_data[string_fields.columns] = string_fields.apply(lambda x: x.str.strip())
        
        downloaded_data.reset_index()
        
        
        print('Survey list downloaded')
        return downloaded_data
        

    def getSurveyYearList(self, survey):
        """
        Returns a dataframe whith the survey and year information of the specified surveys.
        Multiple datasets can be downloaded at once.
        Parameters:
            survey: one or many (as a list) survey names (SP-ARSA, IBTS...)
        Examples:
            survey_year_data = pydatras.getSurveyYearList(['SP-ARSA', 'SP-NORTH'])
            survey_year_data = pydatras.getSurveyYearList(['SP-ARSA'])
        """
        
        if self.datras_client is None:
            print ('DATRAS client not set')
            return
        
        if not isinstance(survey, list):
            survey = [survey]
                    
        downloaded_data = pd.DataFrame()
        
        downloaded = 0
        
        datasets_number = len(survey)
        
        for srv in survey:
            try:
                data = self.datras_client.service.getSurveyYearList(srv)
                if data is not None:
                    downloaded_data = downloaded_data.append(serialize_object(data))
                    downloaded += 1
            except:
                pass
        
        # Cleaning the dataframe
        
        string_fields = downloaded_data.select_dtypes(['object'])
        
        downloaded_data[string_fields.columns] = string_fields.apply(lambda x: x.str.strip())
        
        downloaded_data.reset_index()
        
        print("%i out of %i surveys downloaded" %(downloaded, datasets_number))
        return downloaded_data


    def getSurveyYearQuarterList(self, survey, year, limit_download = True):
        """
        Returns a dataframe whith the survey, year and quarter information of the specified surveys.
        Multiple datasets can be downloaded at once.
        Parameters:
            survey: one or many (as a list) survey names (SP-ARSA, IBTS...)
            year: one or many (as a list) years (2010, 2012...)
        Examples:
            survey_y_q_data = pydatras.getHLdata(['SP-ARSA', 'SP-NORTH'], 2012)
            survey_y_q_data = pydatras.getHLdata(['SP-ARSA'], list(range(2010, 2019)))
        """
        if self.datras_client is None:
            print ('DATRAS client not set')
            return
        
        if not isinstance(survey, list):
            survey = [survey]
        
        if not isinstance(year, list):
            year = [year]
            
        datasets = [ params for params in product(survey, year)]
                
        datasets_number = len(datasets)
        
        if limit_download and (datasets_number > self.download_limits):
            print("Data download is limited to %i datasets." % self.download_limits)
            print("Your are trying to download %i datasets. Exiting" % datasets_number)
            return
        
        downloaded_data = pd.DataFrame()
        
        downloaded = 0
        
        for dataset in datasets:
            try:
                data = self.datras_client.service.getSurveyYearQuarterList(dataset[0], dataset[1])
                if data is not None:
                    downloaded_data = downloaded_data.append(serialize_object(data))
                    downloaded += 1
            except:
                pass
        
        # Cleaning the dataframe
        
        string_fields = downloaded_data.select_dtypes(['object'])
        
        downloaded_data[string_fields.columns] = string_fields.apply(lambda x: x.str.strip())
        
        downloaded_data.reset_index()
        
        print("%i out of %i Datasets downloaded" %(downloaded, datasets_number))
        return downloaded_data

    def getIndices(self, survey, year, quarter, species, limit_download = True):
        """
        *** NOT YET IMPLEMENTED ***
        Returns a dataframe with the indices of the specified surveys, years, quarters
        and species.
        Multiple datasets can be downloaded at once, though its total number is limited by the
        parameter download_limits of the class constructor. However, this behavior can be
        overriden with the limit_download parameter in the function call.
        Parameters:
            survey: one or many (as a list) survey names (SP-ARSA, IBTS...)
            year: one or many (as a list)  years as integers (2010, 2011...)
            quarter: one or many (as a list) of quarters (1, 2...)
            species: one or many (as a list) WORMS Aphia species code (123456, 123334...)
            limit_download: Boolean indicating whether download limits should be observed or not.
        Examples:
            indices_data = pydatras.getIndices(['SP-ARSA', 'SP-NORTH'], 2010, 3, 123456)
            indices_data = pydatras.getIndices(['SP-ARSA'], list(range(2010,2019)), 1,[123456,123323], 'limit_download = False)
        """
        print("getIndices not yet implemented")

    def getLitterAssessmentOutput(self, survey, year, quarter, limit_download = True):
        """
        *** NOT YET IMPLEMENTED ***
        Returns a litter assesment for the specified surveys, years and quarters.
        Multiple datasets can be downloaded at once, though its total number is limited by the
        parameter download_limits of the class constructor. However, this behavior can be
        overriden with the limit_download parameter in the function call.
        Parameters:
            survey: one or many (as a list) survey names (SP-ARSA, IBTS...)
            year: one or many (as a list)  years as integers (2010, 2011...)
            quarter: one or many (as a list) of quarters (1, 2...)
            limit_download: Boolean indicating whether download limits should be observed or not.
        Examples:
            litter_data = pydatras.getLitterAssessmentOutput(['SP-ARSA', 'SP-NORTH'],2010,[1, 2])
            litter_data = pydatras.getLitterAssessmentOutput(['SP-ARSA'],list(range(2010,2019)),1, limit_download = False)
        """
        print("getLitterAssessmentOutput not yet implemented")

    def getLitterAssessmentOutputByUpdateDate(self, dateofcalculation, limit_download = True):
        """
        *** NOT YET IMPLEMENTED ***
        Creates a dataframe with Litter assessment units for the given dateofcalculation.
        Parameters:
            dateofcalculation: one or many (as a list) dates of calculation (format: YYYYMMDD)
        Example:
            litter = pydatras.getLitterAssessmentOutputByUpdateDate(20170204)
        """
        print("getLitterAssessmentOutputByUpdateDate not yet implemented")

    def getListofDateofCalculation(self):
        """
        *** NOT YET IMPLEMENTED ***
        Creates a dataframe with the list of dates of calculation of data productos
        Example:
            date_lists = pydatras.getListofDateofCalculation()
        """
        print("getListofDateofCalculation not yet implemented")

    def getLengthAgeSummary(self, country, year, limit_download = True):
        """
        *** NOT YET IMPLEMENTED ***
        Creates a DataFrame with the length-age summary for the countries and years specified
        Parameters:
            country: one or many (as a list) of countries (ESP, DAN)
            year: one or many (as a list) years (2010, 2011...)
        Examples:
            length_summary = pydatras.getLengthAgeSummary('ESP',2010)
            length_summary = pydatras.getLengthAgeSummary('ESP',[2010,2011,2012])
        """
        print("getLengthAgeSummary not yet implemented")
        
        