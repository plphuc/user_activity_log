import pandas as pd 
import random as rd
from datetime import timedelta
from datetime import datetime
import calendar

def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = rd.randrange(int_delta)
    return start + timedelta(seconds=random_second)

df = pd.read_csv('userInfo.csv')
uidArray = df['uid'].values

for i in range(1, 32):
    new_df = pd.DataFrame(columns=['uid', 'time', 'action'])
    d1 = datetime.strptime(f'{i}/1/2024 12:00 AM', '%d/%m/%Y %I:%M %p')
    d2 = datetime.strptime(f'{i}/1/2024 11:59 PM', '%d/%m/%Y %I:%M %p')
    for _ in range(rd.randint(40000, 60000)):
        uid = rd.choice(uidArray)
        time = random_date(d1, d2).strftime('%d/%m/%Y %I:%M %p')
        time = calendar.timegm(random_date(d1, d2).timetuple())
        action = rd.randint(1,3) #['view', "purchase", "text"]
        new_df.loc[len(new_df.index)] = [uid, time, action]
    new_df.to_csv(f'2024/01/log{i}.csv', index=False)
