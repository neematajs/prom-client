'use strict';

/**
 * Extends the Registry class with a `clusterMetrics` method that returns
 * aggregated metrics for all workers.
 *
 * In cluster workers, listens for and responds to requests for metrics by the
 * cluster master.
 */

const Registry = require('./registry');
// We need to lazy-load the 'cluster' module as some application servers -
// namely Passenger - crash when it is imported.
let cluster = () => {
	const data = require('cluster');
	cluster = () => data;
	return data;
};

const GET_METRICS_REQ = 'prom-client:getMetricsReq';
const GET_METRICS_RES = 'prom-client:getMetricsRes';

let registries = [Registry.globalRegistry];
let requestCtr = 0; // Concurrency control
let listenersAdded = false;
const requests = new Map(); // Pending requests for workers' local metrics.

class ClusterRegistry extends Registry {
	constructor(regContentType = Registry.PROMETHEUS_CONTENT_TYPE) {
		super(regContentType);
		addListeners();
	}

	/**
	 * Gets aggregated metrics for all workers.
	 * @returns {Promise<string>} Promise that resolves with the aggregated
	 *   metrics.
	 */
	clusterMetrics() {
		const requestId = requestCtr++;

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

			const connectedWorkers = getConnectedWorkers();
			const request = {
				responses: new Array(connectedWorkers.length),
				responseIndex: 0,
				responseLength: 0,
				pending: connectedWorkers.length,
				contentType: this.contentType,
				done,
				errorTimeout: setTimeout(() => {
					const err = new Error('Operation timed out.');
					request.done(err);
				}, 5000),
			};
			requests.set(requestId, request);

			const message = {
				type: GET_METRICS_REQ,
				requestId,
			};
			for (let i = 0; i < connectedWorkers.length; i++) {
				connectedWorkers[i].send(message);
			}

			if (request.pending === 0) {
				// No workers were up
				clearTimeout(request.errorTimeout);
				process.nextTick(() => done(undefined, ''));
			}
		});
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

function getConnectedWorkers() {
	const workers = cluster().workers;
	let connectedCount = 0;

	for (const id in workers) {
		if (workers[id].isConnected()) {
			connectedCount++;
		}
	}

	const connectedWorkers = new Array(connectedCount);
	let index = 0;
	for (const id in workers) {
		const worker = workers[id];
		// If the worker exits abruptly, it may still be in the workers
		// list but not able to communicate.
		if (worker.isConnected()) {
			connectedWorkers[index++] = worker;
		}
	}

	return connectedWorkers;
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
		process.send({
			type: GET_METRICS_RES,
			requestId,
			error: error.message,
		});
		return;
	}

	if (metrics instanceof Promise) {
		metrics
			.then(resolvedMetrics => {
				process.send({
					type: GET_METRICS_RES,
					requestId,
					metrics: resolvedMetrics,
				});
			})
			.catch(error => {
				process.send({
					type: GET_METRICS_RES,
					requestId,
					error: error.message,
				});
			});
		return;
	}

	process.send({
		type: GET_METRICS_RES,
		requestId,
		metrics,
	});
}

/**
 * Adds event listeners for cluster aggregation. Idempotent (safe to call more
 * than once).
 * @returns {void}
 */
function addListeners() {
	if (listenersAdded) return;
	listenersAdded = true;

	if (cluster().isPrimary) {
		// Listen for worker responses to requests for local metrics
		cluster().on('message', (worker, message) => {
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
	}

	if (cluster().isWorker) {
		// Respond to master's requests for worker's local metrics.
		process.on('message', message => {
			if (message.type === GET_METRICS_REQ) {
				sendMetricsResponse(message.requestId);
			}
		});
	}
}

module.exports = ClusterRegistry;
