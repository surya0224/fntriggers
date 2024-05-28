from datetime import datetime, timedelta
from dateutil.parser import parse
b = parse('2017-06-01')
print int(b.strftime('%m'))
