import sys
import time

import pymongo
from jupiter_AI import client, JUPITER_DB
db = client[JUPITER_DB]


MONGO_COLLECTION = "JUP_DB_Data_Change_Triggers"


collection = db[MONGO_COLLECTION]

# Get a tailable cursor for our looping fun
cursor = collection.find({'processed': False},
                         cursor_type=pymongo.cursor.CursorType.TAILABLE_AWAIT)

# This will catch ctrl-c and the error thrown if
# the collection is deleted while this script is
# running.
try:

  # The cursor should remain alive, but if there
  # is nothing in the collection, it dies after the
  # first loop. Adding a single record will
  # keep the cursor alive forever as I expected.
  while cursor.alive:
    try:
      message = cursor.next()
      print message
      db[collection].update({'_id': message['_id']}, {'$set': {'processed': True}}, upsert=True)
    except StopIteration:
      time.sleep(1)

except pymongo.errors.OperationFailure:
  print "Delete the collection while running to see this."

except KeyboardInterrupt:
  print "trl-C Ya!"
  sys.exit(0)

print "and we're out"