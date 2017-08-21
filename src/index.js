import amqplib from 'amqplib';
import R from 'ramda';
import nodeuuid from 'uuid';

export default class AMQPUtils {
	constructor(connectionString){
		this._ready = amqplib
			.connect(connectionString)
			.then(conn => {
				this.conn = conn;
				return conn.createChannel();
			})
			.then(async ch => {
				this.chan = ch;
				ch.assertExchange('logs', 'topic', {durable: true});
				ch.assertExchange('events', 'topic', {durable: true});
				const {queue} = await ch.assertQueue('', {exclusive: true});
				this.cbQueue = queue;
			});
	}

	async send(queueName, msg, options = {}, chan){
		const str = R.is(Object, msg) ? JSON.stringify(msg) : msg.toString();
		await this._ready;
		return (chan || this.chan).sendToQueue(queueName, new Buffer(str), options);
	}

	async consume(queueName, handler, options = {}, chan){
		await this._ready;
		return (chan || this.chan).consume(queueName, handler, {noAck: true, ...options});
	}

	async publish(exchange, topic, msg, options){
		await this._ready;
		const str = R.is(Object, msg) ? JSON.stringify(msg) : msg.toString();
		return this.chan.publish(exchange, topic, new Buffer(str), options);
	}

	async subscribe(exchange, topic, handler){
		await this._ready;
		const {queue} = await this.chan.assertQueue('', {exclusive: true});
		this.chan.bindQueue(queue, exchange, topic);
		return this.consume(queue, handler);
	}

	async log(topic, msg, options){
		await this._ready;
		return this.publish('logs', topic, msg, options);
	}

	async emit(topic, msg, options){
		await this._ready;
		return this.publish('events', topic, msg, options);
	}

	async on(topic, handler){
		await this._ready;
		return this.subscribe('events', topic, handler);
	}

	async consumeEventStream(topic, handler){
		await this._ready;
		const {queue} = await this.chan.assertQueue('', {exclusive: true});
		this.call('GetEventStream', {topic, replyTo: queue});
		this.chan.bindQueue(queue, 'events', topic);
		return this.consume(queue, handler);
	}

	async apply(methodName, args = []){
		await this._ready;
		const correlationId = nodeuuid.v1();
		this.send(`RPC:${methodName}`, args, {replyTo: this.cbQueue, correlationId});
		console.log(`RPC:${methodName}`, args);
		return new Promise(async (resolve, reject) => {
			const {consumerTag} = await this.consume(this.cbQueue, msg => {
				const {properties, content} = msg;
				if (properties.correlationId == correlationId){
					resolve(R.head(JSON.parse(content.toString())));
					this.chan.cancel(consumerTag);
				}
			});
		});
	}

	async call(methodName, ...args){
		return this.apply(methodName, args);
	}

	async method(methodName, handler, prefetch = 1){
		await this._ready;
		const chan = await this.conn.createChannel();
		chan.prefetch(prefetch);
		this.consume(`RPC:${methodName}`, async msg => {
			const {content, properties} = msg;
			const {replyTo, correlationId} = properties;
			const args = JSON.parse(content.toString());
			const result = await handler(...args);
			chan.ack(msg);
			this.send(replyTo, [result], {correlationId}, chan);
		}, {noAck: false}, chan);
	}

	methods(obj){
		for (const key in obj){
			this.method(key, obj[key]);
		}
	}
}