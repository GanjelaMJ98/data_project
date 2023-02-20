import pandas as pd

def meetings_preprocessor(meetings, addressbook):

    meetings_initiators = meetings.copy()
    meetings_initiators['member'] = meetings_initiators['initiator']
    meetings = meetings.append(meetings_initiators)
    meetings = meetings.drop_duplicates(subset=['meeting_id','start_time','member'])

    meetings = meetings.merge(addressbook, how='left', left_on='member', right_on='email')

    agg_tabnums = meetings.groupby(['meeting_id','start_time'])['tabnum'].apply(list).reset_index()
    agg_tabnums = agg_tabnums.rename(columns={'tabnum':'participants_tabnums'})
    agg_emails = meetings.groupby(['meeting_id','start_time'])['email'].apply(list).reset_index()
    agg_emails = agg_emails.rename(columns={'email':'participants_emails'})
    meetings = meetings.merge(agg_tabnums, on=['meeting_id','start_time'])
    meetings = meetings.merge(agg_emails, on=['meeting_id','start_time'])

    meetings['participants_cnt'] = meetings['participants_emails'].apply(len)
    meetings['duration'] = (meetings['end_time'] - meetings['start_time']).astype('timedelta64[m]')
    
    return meetings


if __name__ == '__main__':
    addressbook = pd.read_excel(r'C:\Users\ganje\Pasha\Code\SmartC_dev\testcase.xlsx',sheet_name='addressbook')
    meetings = pd.read_excel(r'C:\Users\ganje\Pasha\Code\SmartC_dev\testcase.xlsx',sheet_name='meetings')


