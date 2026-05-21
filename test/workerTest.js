'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert');
const Path = require('path');
const { once } = require('events');
const { Worker, BroadcastChannel } = require('node:worker_threads');

const Registry = require('../lib/registry');
const WorkerRegistry = require('../lib/worker');

describe('WorkerRegistry', () => {
	it('works properly if there are no worker threads', async () => {
		const registry = new WorkerRegistry();
		const metrics = await registry.workerMetrics();
		assert.strictEqual(metrics, '');
	});

	it('validates custom registries', () => {
		assert.throws(
			() => {
				WorkerRegistry.setRegistries({});
			},
			{
				name: 'TypeError',
				message: 'Expected Registry, got object',
			},
		);
	});

	it('does not error out on unexpected (or late) responses', async () => {
		const registry = new WorkerRegistry();
		const channelName = `prom-client:test:${process.pid}`;
		const channel = new BroadcastChannel(channelName);
		registry.addWorker(channelName);

		channel.postMessage({
			type: 'prom-client:getMetricsRes',
			metrics: [],
			requestId: -3,
		});

		await new Promise(resolve => {
			setImmediate(resolve);
		});
		channel.close();
	});

	it('aggregates metrics from a worker thread', async () => {
		const workerRegistry = new WorkerRegistry();
		const worker = new Worker(
			`
			'use strict';

			const { parentPort, workerData } = require('node:worker_threads');
			const client = require(workerData.clientPath);
			const registry = new client.Registry();
			const counter = new client.Counter({
				name: 'worker_test_counter',
				help: 'Worker test counter',
				labelNames: ['worker'],
				registers: [registry],
			});

			counter.inc({ worker: 'one' }, 2);
			client.WorkerRegistry.setRegistries(registry);
			new client.WorkerRegistry(client.Registry.PROMETHEUS_CONTENT_TYPE, false);
			parentPort.postMessage('ready');
			setInterval(() => {}, 1000);
			`,
			{
				eval: true,
				workerData: {
					clientPath: Path.join(__dirname, '..'),
				},
			},
		);

		workerRegistry.addWorker(worker);
		await once(worker, 'message');

		const metrics = await workerRegistry.workerMetrics();
		assert.match(metrics, /# HELP worker_test_counter Worker test counter/);
		assert.match(metrics, /# TYPE worker_test_counter counter/);
		assert.match(metrics, /worker_test_counter\{worker="one"\} 2/);

		await worker.terminate();
	});
});

describe('WorkerRegistry.aggregate()', () => {
	it('delegates to Registry.aggregate()', () => {
		const metrics = [
			[
				{
					name: 'test_metric',
					help: 'Test metric',
					type: 'gauge',
					values: [{ value: 1, labels: {} }],
					aggregator: 'sum',
				},
			],
			[
				{
					name: 'test_metric',
					help: 'Test metric',
					type: 'gauge',
					values: [{ value: 2, labels: {} }],
					aggregator: 'sum',
				},
			],
		];

		const aggregated = WorkerRegistry.aggregate(metrics);
		assert.ok(aggregated instanceof Registry);
		assert.deepStrictEqual(aggregated.getSingleMetric('test_metric').get(), {
			name: 'test_metric',
			help: 'Test metric',
			type: 'gauge',
			values: [{ value: 3, labels: {} }],
			aggregator: 'sum',
		});
	});
});
