from confluent_kafka import Consumer
import json
c = Consumer({
	'bootstrap.servers': 'localhost:9092',
	'group.id': 'mygroup',
	#'auto.offset.reset': 'latest'
	'default.topic.config': {
		'auto.offset.reset': 'smallest'
	},
	'enable.auto.commit': 'false'
	#,
	#'max.poll.records': '100'
})
try:
	c.subscribe(['sample-1'])
	transaction_list = []
	while True:
		msg = c.poll(1.0)
		if msg is None:
			continue
		if msg.error():
			print("Consumer error: {}".format(msg.error()))
			continue
		xxx = json.loads(msg.value().decode('utf-8'))
		#print(type(xxx))
		#print(xxx["data"]["price"])
		transaction_list.append(xxx["data"])
		transaction_list = sorted(transaction_list, key = lambda i: i['price'], reverse=True)
		#print(transaction_list[0:10])
		if len(transaction_list) > 100:
			print("Lenlist: ", len(transaction_list))
			print("Top 10 transactions by Price")
			for i in range(0, 10):
				print(transaction_list[i]["id"], transaction_list[i]["price"])
			print("\n\n\n")
			c.commit(asynchronous=False)
			transaction_list = []
except KeyboardInterrupt:
	print("Press Ctrl-C to terminate while statement")
	pass
except Exception as ex:
	print("Something happened", ex)
finally:
	# Close down consumer to commit final offsets.
	c.close()