topic 
[topic messageid] message

topic_head
topic messageid

consumer_head
[topic consumer] messageid

consumer_processing
[topic consumer messageid key nodeid] status,updated 

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
- Then waits for new threads
- Grabs as many messages as it can handle immediately
- Inserts to processing and moves the head up
- Fetches unprocessed and assigns to processors (include errored some time ago)
- Loops

Processor loop
- Receives message
- Checks for messages with the same key (does nothing with message if unprocessed keys exist)
- processes message
- deletes status record when done
- if error, retry immediately, then updates status to error

Controller loop
- sets its own hearbeat
- looks for dead nodes (no timestamp for x ms)
- resets processing messages for dead nodes
- removes dead nodes

