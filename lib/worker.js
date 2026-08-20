'use strict';

/**
 * Extends the Registry class with a `workerMetrics` method that returns
 * aggregated metrics for all worker threads.
 *
 * In worker threads, listens for and responds to requests for metrics by the
 * collector thread.
 */

const Registry = require('./registry');
const {
	isMainThread,
	threadId,
	BroadcastChannel,
} = require('node:worker_threads');

const ANNOUNCEMENT = 'prom-client:announcement';
const GET_METRICS_REQ = 'prom-client:getMetricsReq';
const GET_METRICS_RES = 'prom-client:getMetricsRes';
const WORKER_DISCOVERY_TIMEOUT = 100;
const ANNOUNCEMENT_CHANNEL = new BroadcastChannel('prom-client:announce');

ANNOUNCEMENT_CHANNEL.unref();

let registries = [Registry.globalRegistry];
let requestCtr = 0; // Concurrency control
let listenersAdded = false;
let localChannel;
const requests = new Map(); // Pending requests for workers' local metrics.
const primaryRegistries = new Set();

class WorkerRegistry extends Registry {
	/**
	 * Create a Registry.
	 * If set to primary, this thread will coordinate metrics collection from
	 * other worker threads.
	 * @param {string} regContentType registry content type.
	 * @param {boolean} primary whether this is the coordinating thread.
	 */
	constructor(
		regContentType = Registry.PROMETHEUS_CONTENT_TYPE,
		primary = isMainThread,
	) {
		super(regContentType);
		this.primary = primary;

		if (this.primary) {
			this.channels = new Map();
			primaryRegistries.add(this);
		}

		addListeners();
		announce(getChannelName(), this.primary);
	}

	/**
	 * Add a worker thread to the aggregation list.
	 * @param {string|object} worker Worker instance or channel name.
	 * @returns {void}
	 */
	addWorker(worker) {
		if (!this.primary) {
			return;
		}

		const name = getWorkerChannelName(worker);
		if (this.channels.has(name)) {
			return;
		}

		const channel = new BroadcastChannel(name);
		channel.unref();
		channel.addEventListener('message', event => {
			const message = event.data;

			if (message.type === GET_METRICS_RES) {
				const request = requests.get(message.requestId);

				if (request === undefined) {
					return;
				}

				if (message.error) {
					request.done(new Error(message.error));
					return;
				}

				request.responses[request.responseIndex++] = message.metrics;
				request.responseLength += message.metrics.length;
				request.pending--;

				if (request.pending === 0) {
					// finalize
					clearTimeout(request.errorTimeout);

					const registry = Registry.aggregate(
						flattenResponses(request.responses, request.responseLength),
						request.contentType,
					);
					const promString = registry.metrics();
					request.done(undefined, promString);
				}
			}
		});

		this.channels.set(name, channel);

		if (typeof worker === 'object' && typeof worker.once === 'function') {
			worker.once('exit', () => {
				channel.close();
				this.channels.delete(name);
			});
		}
	}

	/**
	 * Gets aggregated metrics for all worker threads.
	 * @returns {Promise<string>} Promise that resolves with the aggregated
	 *   metrics.
	 */
	async workerMetrics() {
		if (this.primary) {
			await this.discoverWorkers();
		}

		const requestId = requestCtr++;
		const channelCount = this.primary ? this.channels.size : 0;

		return new Promise((resolve, reject) => {
			let settled = false;
			function done(err, result) {
				if (settled) return;
				settled = true;

				requests.delete(requestId);

				if (err !== undefined) {
					reject(err);
				} else {
					resolve(result);
				}
			}

			const request = {
				responses: new Array(channelCount),
				responseIndex: 0,
				responseLength: 0,
				pending: channelCount,
				contentType: this.contentType,
				done,
				errorTimeout: setTimeout(() => {
					const err = new Error(
						`Operation timed out. ${request.pending} outstanding responses.`,
					);
					request.done(err);
				}, 5000),
			};
			requests.set(requestId, request);

			ANNOUNCEMENT_CHANNEL.postMessage({
				type: GET_METRICS_REQ,
				threadId,
				requestId,
			});

			if (request.pending === 0) {
				// No workers were up
				clearTimeout(request.errorTimeout);
				process.nextTick(() => done(undefined, ''));
			}
		});
	}

	/**
	 * Refresh worker channels before collecting metrics.
	 *
	 * Workers discovered by announcement do not provide an exit event, so a
	 * long-running controller otherwise retains channels for workers replaced
	 * by a reload and waits forever for their responses.
	 * @returns {Promise<void>} Promise resolved after live workers re-announce.
	 */
	async discoverWorkers() {
		for (const channel of this.channels.values()) {
			channel.close();
		}
		this.channels.clear();

		announce(getChannelName(), true);
		await new Promise(resolve => setTimeout(resolve, WORKER_DISCOVERY_TIMEOUT));
	}

	get contentType() {
		return super.contentType;
	}

	/**
	 * Creates a new Registry instance from an array of metrics that were
	 * created by `registry.getMetricsAsJSON()`. Metrics are aggregated using
	 * the method specified by their `aggregator` property, or by summation if
	 * `aggregator` is undefined.
	 * @param {Array} metricsArr Array of metrics, each of which created by
	 *   `registry.getMetricsAsJSON()`.
	 * @param {string} registryType content type of the new registry. Defaults
	 * to PROMETHEUS_CONTENT_TYPE.
	 * @returns {Registry} aggregated registry.
	 */
	static aggregate(
		metricsArr,
		registryType = Registry.PROMETHEUS_CONTENT_TYPE,
	) {
		return Registry.aggregate(metricsArr, registryType);
	}

	/**
	 * Sets the registry or registries to be aggregated. Call from workers to
	 * use a registry/registries other than the default global registry.
	 * @param {Array<Registry>|Registry} regs Registry or registries to be
	 *   aggregated.
	 * @returns {void}
	 */
	static setRegistries(regs) {
		if (!Array.isArray(regs)) regs = [regs];
		for (let i = 0; i < regs.length; i++) {
			const reg = regs[i];
			if (!(reg instanceof Registry)) {
				throw new TypeError(`Expected Registry, got ${typeof reg}`);
			}
		}
		registries = regs;
	}
}

function getChannelName() {
	return `prom-client:worker:${threadId}`;
}

function getWorkerChannelName(worker) {
	if (typeof worker === 'string') {
		return worker;
	}

	return `prom-client:worker:${worker.threadId}`;
}

function collectRegistriesMetrics() {
	const results = new Array(registries.length);
	let hasPromise = false;

	for (let i = 0; i < registries.length; i++) {
		const result = registries[i].getMetricsAsJSON();
		results[i] = result;
		if (result instanceof Promise) {
			hasPromise = true;
		}
	}

	if (hasPromise) {
		return Promise.all(results);
	}

	return results;
}

function flattenResponses(responseGroups, totalLength) {
	const responses = new Array(totalLength);
	let index = 0;
	for (let i = 0; i < responseGroups.length; i++) {
		const group = responseGroups[i];
		if (group === undefined) {
			continue;
		}

		for (let j = 0; j < group.length; j++) {
			responses[index++] = group[j];
		}
	}

	return responses;
}

function sendMetricsResponse(requestId) {
	let metrics;
	try {
		metrics = collectRegistriesMetrics();
	} catch (error) {
		localChannel.postMessage({
			type: GET_METRICS_RES,
			requestId,
			threadId,
			error: error.message,
		});
		return;
	}

	if (metrics instanceof Promise) {
		metrics
			.then(resolvedMetrics => {
				localChannel.postMessage({
					type: GET_METRICS_RES,
					requestId,
					threadId,
					metrics: resolvedMetrics,
				});
			})
			.catch(error => {
				localChannel.postMessage({
					type: GET_METRICS_RES,
					requestId,
					threadId,
					error: error.message,
				});
			});
		return;
	}

	localChannel.postMessage({
		type: GET_METRICS_RES,
		requestId,
		threadId,
		metrics,
	});
}

/**
 * Watch for metrics collection events.
 * @returns {void}
 */
function addListeners() {
	if (listenersAdded) {
		return;
	}

	listenersAdded = true;
	localChannel = new BroadcastChannel(getChannelName());
	localChannel.unref();

	ANNOUNCEMENT_CHANNEL.addEventListener('message', event => {
		const message = event.data;

		if (message.type === ANNOUNCEMENT) {
			if (primaryRegistries.size > 0) {
				if (!message.primary) {
					for (const registry of primaryRegistries) {
						registry.addWorker(message.name);
					}
				}
			} else if (message.primary) {
				announce(getChannelName(), false);
			}
		} else if (message.type === GET_METRICS_REQ) {
			if (primaryRegistries.size === 0) {
				sendMetricsResponse(message.requestId);
			}
		}
	});
}

function announce(name, primary) {
	ANNOUNCEMENT_CHANNEL.postMessage({
		type: ANNOUNCEMENT,
		name,
		threadId,
		primary,
	});
}

module.exports = WorkerRegistry;
