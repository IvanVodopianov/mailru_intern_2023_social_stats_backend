# -*- coding: utf-8 -*-

class Event_Repository:
    
    def __init__(self):
        self.repository = dict()
        
    def save(self, value):
        key = len(self.repository)
        self.repository[key] = value
        
    def fetch_all(self):
        for e in sorted(self.repository):
            print(e,self.repository[e])
            
    def __iter__(self):
        self.cur_val = 0
        return self
    
    def __next__(self):
        tmp = self.cur_val
        if tmp > len(self.repository) - 1:
            raise StopIteration
        self.cur_val += 1
        return tmp
    
    def __getitem__(self, key):
        return self.repository[key]

def save_event (db, event_name, user_status, **kwargs):
    
    if 'IP_ADDRESS' in kwargs: user_ip_address = kwargs['IP_ADDRESS']
    else: user_ip_address = ''
    
    if 'EVENT_DATE' in kwargs: event_date = kwargs['EVENT_DATE']
    else: event_date = ''
    
    data_to_save = {'event_name':event_name, 'event_date':event_date, 'user_status':user_status, 'user_ip_address':user_ip_address}
    
    db.save(data_to_save)

def filter_and_aggregate (db, **kwargs):
    import json
    
    if 'FILTER_BY_DATE_FUNCTION' in kwargs:  filter_by_date = kwargs['FILTER_BY_DATE_FUNCTION']
    else: filter_by_date = lambda x: False
    
    if 'FILTER_BY_EVENT_NAME_FUNCTION' in kwargs:  filter_by_event_name = kwargs['FILTER_BY_EVENT_NAME_FUNCTION']
    else: filter_by_event_name = lambda x: False
    
    events_counters, ip_counters, statuses_counters = False, False, False
    
    if 'AGG_FUNCTION' in kwargs:
        
        if kwargs['AGG_FUNCTION']   == 'COUNTERS_FOR_EVENTS':
            events_counters = True
            
        elif kwargs['AGG_FUNCTION'] == 'COUNTERS_FOR_IP_ADDRESSES':
            ip_counters = True
            
        elif kwargs['AGG_FUNCTION'] == 'COUNTERS_FOR_USERS_STATUSES':
            statuses_counters = True
    
    if events_counters:
        result = dict()
        for key in db:
            if not filter_by_date ( db[key]['event_date'] ): continue
            if not filter_by_event_name ( db[key]['event_name'] ): continue

            k = db[key]['event_name']
            
            if not k in result: result[k] = 0
            
            result[k] +=1
            
    if ip_counters:
        result = dict()
        for key in db:
            if not filter_by_date ( db[key]['event_date'] ): continue
            if not filter_by_event_name ( db[key]['event_name'] ): continue

            k = db[key]['user_ip_address']
            
            if not k in result: result[k] = 0
            
            result[k] +=1
            
    if statuses_counters:
        result = dict()
        for key in db:
            if not filter_by_date ( db[key]['event_date'] ): continue
            if not filter_by_event_name ( db[key]['event_name'] ): continue

            k = db[key]['user_status']
            
            if not k in result: result[k] = 0
            
            result[k] +=1
    
    return json.dumps(result)
    
    
if __name__ == '__main__':
    
    event_repository = Event_Repository()
    
    save_event (        db = event_repository,
               event_name  = 'registration',
               user_status = 'unauthorized',
               IP_ADDRESS  = '127.0.0.1',
               EVENT_DATE  = '2022-01-01'
               )
    
    save_event (        db = event_repository,
               event_name  = 'change_password',
               user_status = 'authorized'
               )
    event_repository.fetch_all()
    
    print(filter_and_aggregate (db = event_repository,
                      AGG_FUNCTION = 'COUNTERS_FOR_EVENTS',
     FILTER_BY_EVENT_NAME_FUNCTION = lambda name: True,
     FILTER_BY_DATE_FUNCTION = lambda date:  date >= '2022-01-01' and date <= '2022-12-31' or date == '' )
          )
    
    print(filter_and_aggregate (db = event_repository,
                      AGG_FUNCTION = 'COUNTERS_FOR_IP_ADDRESSES',
     FILTER_BY_EVENT_NAME_FUNCTION = lambda name: True,
     FILTER_BY_DATE_FUNCTION = lambda date:  date >= '2022-01-01' and date <= '2022-12-31' or date == '' )
          )
    
    print(filter_and_aggregate (db = event_repository,
                      AGG_FUNCTION = 'COUNTERS_FOR_USERS_STATUSES',
     FILTER_BY_EVENT_NAME_FUNCTION = lambda name: True,
           FILTER_BY_DATE_FUNCTION = lambda date:  date >= '2022-01-01' and date <= '2022-12-31' or date == '' )
          )