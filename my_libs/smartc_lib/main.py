import pandas as pd
from smartc_lib.meetings.preprocessor import meetings_preprocessor


def read_meetings_preprocess_and_save():

    addressbook = pd.read_excel(r'C:\Users\ganje\Pasha\Code\SmartC_dev\data\testcase.xlsx',sheet_name='addressbook')
    raw_meetings = pd.read_excel(r'C:\Users\ganje\Pasha\Code\SmartC_dev\data\testcase.xlsx',sheet_name='meetings')

    prep_meetings = meetings_preprocessor(raw_meetings, addressbook)
    prep_meetings.to_excel(r'C:\Users\ganje\Pasha\Code\SmartC_dev\data\result.xlsx', index=False)
    return 1