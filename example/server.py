from stacks import iter_traceback_frames, get_stack_info
import sys
import requests
from datetime import datetime
import random
import time

def func1(a, *args, **kwargs):
    d = {'a': 3, 'b': func2}
    raise Exception("Test Exception {}".format(random.randint(1,10)))

def func2(a):
    return func1(a, [1,2,3], hello={'a':2,'b':3})

def func3(a):
    return func2(a)

def func4(a):
    return func3(a)

if __name__ == '__main__':

    services = ['wish_fe','wish_be','merchant_fe','merchant_be','default']
    environments = ['prod','stage','dev','sandbox', 'default']
    event_types = ['python', 'go', 'ruby', 'c', 'c++', 'c#', 'java', 'javascript']

    while True:
        try:
            print func4(3)
        except Exception as e:
            exc_typ, exc_value, tb = sys.exc_info()
            data = {}
            data['timestamp'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            data['service'] = random.choice(services)
            data['environment'] = random.choice(environments)
            data['event_type'] = random.choice(event_types)
            data['event_name'] = exc_typ.__name__
            data['extra_args'] = {}
            data['event_data'] = {
                'message': exc_value.message,
                'raw_data': get_stack_info(iter_traceback_frames(tb))
            }
            data['configurable_filters'] = {
                'base': ['exception_python_remove_line_no', 'exception_python_remove_stack_vars'],
                'instance': ['exception_python_process_stack_vars']
            }
            data['configurable_groupings'] = []

            count = random.randint(50,500)
            for i in range (1, count):
                res = requests.post('http://0.0.0.0:8080/capture', json=data)
                print res
        interval = random.randint(10,50)
        time.sleep(interval)

