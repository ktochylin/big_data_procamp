from confluent_kafka import Consumer
import json
from time import sleep

conf = {'bootstrap.servers': 'localhost:9092',
		'group.id': 'mygroup',
		#'auto.offset.reset': 'latest',
		'default.topic.config': {'auto.offset.reset': 'latest'},
		'enable.auto.commit': 'false'}
		
consumer = Consumer(conf)

topics = ["bitstamp.transaction.btcusd"]

def consume_loop(consumer, topics, commit_count):
	try:
		consumer.subscribe(topics)
		transaction_list = []
		MIN_COMMIT_COUNT = commit_count
		msgno = 0
		while True:
			msg = consumer.poll(1.0)
			if msg is None:
				continue
			if msg.error():
				print("Consumer error: {}".format(msg.error()))
				continue
			else:
				data = json.loads(msg.value().decode('utf-8'))["data"]
				if data != {} & data["order_type"] == 1:
					transaction_list.append(data)
					msgno += 1
					
				if msgno % MIN_COMMIT_COUNT == 0:
					print("Lenlist: ", len(transaction_list))
					print("Top 10 transactions by Price")
					print(msgno)
					transaction_list = sorted(transaction_list, key = lambda i: i['price'], reverse=True)[:10]
					
					for transaction in transaction_list:
						print("{} | {} | {: >16f} | {: f}".format( \
								transaction["id"], \
								transaction["order_type"], \
								transaction["price"], \
								transaction["amount"]))
					
					print("\n\n")
					# at least once
					print("Commit info nessage:")
					consumer.commit(asynchronous=False)
					
					sleep(1)
	except KeyboardInterrupt:
		print("Press Ctrl-C to terminate while statement")
		pass
	except Exception as ex:
		print("Something happened", ex.with_traceback())
		pass
	finally:
		# Close down consumer to commit final offsets.
		consumer.close()

if __name__ == "__main__":
	consume_loop(consumer, topics, 100)
