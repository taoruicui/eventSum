from stacks import iter_traceback_frames, get_stack_info
import sys
from raven import Client
import requests
from datetime import datetime
import traceback

def func1(a, *args, **kwargs):
    d = {'a': 3, 'b': func2}
    return a/0

def func2(a):
    return func1(a, [1,2,3], hello={'a':2,'b':3})

def func3(a):
    return func2(a)

def func4(a):
    return func3(a)

if __name__ == '__main__':
    client = Client('http://008e8e98273346d782db8bc407917e76:dedc6b38b0dd484fa4db1543a19cb0f5@sentry.i.wish.com/2')
    try:
        print func4(3)
    except Exception as e:
        exc_typ, exc_value, tb = sys.exc_info()
        c = client.captureException(id=3, time=datetime.now())
        data = {}
        data['timestamp'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        data['service'] = 'Wish FE'
        data['environment'] = 'PROD'
        data['event_type'] = 'python'
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
        while True:
            res = requests.post('http://0.0.0.0:8080/capture', json=data)
            print res


