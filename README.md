# Realtime-Scorecard-Generator-From-Live-Commentary

## Requirements:-
1. 48 commentary files
	"194161007-match id-commentary.txt"
2. Python v3.x
3. Kafka module for python3
4. Running Kafka Server on default ports

## Steps:-
1. Start the Kafka Server and Zookeeper on defult ports
2. Run Kafka_consumer.py
3. Run Kafka_producer.py

## During execution two new files will be generated for each match
1. "194161007-match id-commentary-consumed.txt"
	Commentary as recieved by the consumer and written into the text file.
2. "194161007-match id-scorecard-computed.txt"
	Scorecard generated from the recieved commentary using the above file.

## Both of these files will get updated in real time during the execution.
## Scorecards will be generated in real time.
