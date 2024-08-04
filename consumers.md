topic 
[topic messageid] message

topic_head
topic messageid

consumer_head
[topic consumer] messageid

consumer_processing
[topic consumer messageid nodeid] status,updated 
key_processing
[topic consumer key] messageid 

consumer_heartbeat
[topic consumer nodeid] timestamp 


consumer grabs p messages, places all in consumer_processing status unprocessed
consumer parallel processes messages
consumer threads update consumer_processing status to processed
consumer thread returns to pool

consumer status
waiting-on-messages (x threads)
waiting-on-threads


no processing
new messages
write messages to processing + move the head up

x processing
new messages
write p-x messages to processing + move the head up

thread looks at key
any unprocessed for this key? move on to next
set to processing in tx
in new tx
- process
- delete from processing
- set toerrored

Consumer loop
- Waits for new messages
- Then waits for new threads (maybe reverse these - dont grab what you cant handle)
- Grabs as many messages as it can handle immediately
- Inserts to processing and moves the head up
- Fetches unprocessed and assigns to processors (include errored some time ago)
  - where topic, consumer, status, nodeid
- Loops

Processor loop
- Receives message
  - where topic, consumer, messageid
- Checks for messages with the same key (does nothing with message if unprocessed keys exist)
  - where topic, consumer, key, messageid, status (key_processing)
- processes message
- deletes status record when done
- if error, retry immediately, then updates status to error

Controller loop
- sets its own hearbeat
- looks for dead nodes (no timestamp for x ms)
- resets processing messages for dead nodes
- removes dead nodes


Processor key ring
- uses consistent hash algorithm - mod of hash should be fine
- has an entry for each mod (1000 threads, 1000 entries)
- when a thread is started for a processor, the mod is added to the 'in use' ring
- subsequent messages with the same mod must wait for the in use to become free
