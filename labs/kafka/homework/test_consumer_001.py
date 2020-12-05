from confluent_kafka import Consumer
import json
from time import sleep
import sys

for i in sys.argv:
	print(f"Name of the script      : {sys.argv[0]=}")

conf = {'bootstrap.servers': 'localhost:9092',
		'group.id': 'mygroup',
		#'auto.offset.reset': 'latest',
		'default.topic.config': {'auto.offset.reset': 'latest'},
		'enable.auto.commit': 'false'}
		
consumer = Consumer(conf)

topics = ["bitstamp.transaction.btcusd"]
top_x_count = 10
top_batch_count = 100

def consume_loop(consumer, topics, top_x, commit_count):
	try:
		consumer.subscribe(topics)
		transaction_list = []
		msgno = 0
		while True:
			msg = consumer.poll(1.0)
			if msg is None:
				continue
			if msg.error():
				print("Consumer error: {}".format(msg.error()))
				continue
			else:
				# Deserialize kafka message
				data = json.loads(msg.value().decode('utf-8'))["data"]
				# Collect messages
				if (data != {}) & (data["order_type"] == 1):
					print(type(data), data["order_type"], type(data["order_type"]))
					transaction_list.append(data)
					msgno += 1
				# Do batch
				if msgno % commit_count == 0:
					print("commit_count + top{} = {}: ".format(top_x, len(transaction_list)), "\n")
					print("Top 10 transactions by Price", "\n")
					# Get Top X
					transaction_list = sorted(transaction_list, key = lambda i: i['price'], reverse=True)[:top_x]
					# Header
					print(" {: >15} | {: >4} | {: >16} | {}".format("transaction id", "type", "price", "amount"))
					# Top X
					for transaction in transaction_list:
						print("{} | {: >4} | {: >16f} | {: f}".format( \
								transaction["id"], \
								transaction["order_type"], \
								transaction["price"], \
								transaction["amount"]))
					
					print("\n\n")
					# at least once (Commit after show Top X)
					# print("Commit info nessage:")
					consumer.commit(asynchronous=False)
					
					sleep(1)
	except KeyboardInterrupt:
		print("Press Ctrl-C to terminate while statement")
		pass
	# If not expected error
	except Exception as ex:
		print("Something happened", ex.with_traceback())
		raise
	finally:
		# Close down consumer to commit final offsets.
		consumer.close()

if __name__ == "__main__":
	consume_loop(consumer, topics, top_x_count, top_batch_count)
