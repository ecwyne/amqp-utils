const amqplib = require('amqplib');
const R = require('ramda');
const nodeuuid = require('node-uuid');
const Promise = require('bluebird');

const Resolvers = {
	json: msg => {
		console.log(msg.content.toString());
		return JSON.parse(msg.content.toString());
	},
	number: msg => Number(msg.content.toString()),
	string: msg => msg.content.toString(),
	date: msg => new Date(msg.content.toString())
};

module.exports = connectionString => {
	const amqp = amqplib.connect(connectionString);
	const chan = amqp.then(conn => conn.createChannel());

	const assertQueue = (queueName, options) => {
		return chan.then(ch => ch.assertQueue(queueName, options));
	};

	const deleteQueue = (queueName, options) => {
		return chan.then(ch => ch.deleteQueue(queueName, options));
	};

	const cancel = consumerTag => chan.then(ch => ch.cancel(consumerTag));

	const send = (queueName, msg, options) => {
		return chan.then(ch => ch.sendToQueue(queueName, new Buffer(JSON.stringify(msg)), options));
	};

	const consume = (queueName, handler, options) => {
		return chan.then(ch => ch.consume(queueName, handler, options));
	};

	const ack = msg => chan.then(ch => ch.ack(msg));

	const uuid = () => nodeuuid.v1();

	let callbackQueue;
	const callbackMessages = () => {
		if (!callbackQueue){
			callbackQueue = assertQueue('', {exclusive: true});
		}
		return callbackQueue;
	};

	const call = (queueName, msg, options = {}) => {
		const correlationId = nodeuuid.v1();
		const p = new Promise((resolve, reject) => {
			const t = setTimeout(() => reject(new Error(queueName + ' request timed out')), options.timeout || 10 * 60 * 1000);
			callbackMessages()
					.tap(q => {
						const fn = res => {
							if (res.properties.correlationId == correlationId){
								resolve(res);
								tagPromise.then(({consumerTag}) => {
									cancel(consumerTag);
								});
							}
						};
						const tagPromise = consume(q.queue, fn, {noAck: true});

					})
					.then(q => send(queueName, msg, R.merge(options, {correlationId, replyTo: q.queue})))
					.tap(() => clearTimeout(t))
					.catch(reject);
		});
		if (options.resolver){
			const f = R.propOr(options.resolver, options.resolver, Resolvers);
			return R.is(Function, f) ? p.then(f) : p;
		} else {
			return p;
		}
	};

	const handle = (queueName, fn, options = {}) => {
		const parseMsg = R.cond([
				[R.is(Function), R.identity],
				[R.has(R.__, Resolvers), R.prop(R.__, Resolvers)],
				[R.T, Resolvers.string]
		])(options.resolver);
		return consume(queueName, msg => {
			const arg = parseMsg(msg);
			Promise.resolve(fn(arg)).then(value => {
				send(msg.properties.replyTo, value, {correlationId: msg.properties.correlationId});
				ack(msg);
			});
		}, options);
	};
	return {assertQueue, deleteQueue, cancel, send, consume, ack, uuid, call, handle, chan};
};