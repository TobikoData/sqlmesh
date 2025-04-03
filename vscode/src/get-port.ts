/**
 * Get port is a typescript translation of the get-port npm package.
 */
import * as net from 'node:net';
import * as os from 'node:os';

class Locked extends Error {
	// Optionally keep the locked port on the error instance
	public port: number;

	constructor(port: number) {
		super(`${port} is locked`);
		this.name = 'Locked';
		this.port = port;
	}
}

interface LockedPorts {
	old: Set<number>;
	young: Set<number>;
}

const lockedPorts: LockedPorts = {
	old: new Set(),
	young: new Set(),
};

// Interval (ms) after which we move `young` to `old` and clear `young`
const releaseOldLockedPortsIntervalMs = 1000 * 15;

const minPort = 1024;
const maxPort = 65535;

// Lazily created timeout reference
let timeout: NodeJS.Timeout | undefined;

// These are the options we accept for getPorts. It extends net.ListenOptions
// so that it can be used in `server.listen(options)`.
export interface GetPortsOptions extends net.ListenOptions {
	/**
	 * Port can be a single number or an iterable of numbers (e.g. number[]).
	 * If not specified, we'll try random or fallback to 0.
	 */
	port?: number | Iterable<number>;
	/**
	 * A set/array/iterable of ports to exclude from consideration.
	 */
	exclude?: Iterable<number>;
}

const getLocalHosts = (): Set<string | undefined> => {
	const interfaces = os.networkInterfaces();

	// Include `undefined` for the default host resolution
	// and '0.0.0.0' to ensure IPv4 usage when the default might pick IPv6.
	const results = new Set<string | undefined>([undefined, '0.0.0.0']);

	for (const iface of Object.values(interfaces)) {
		if (!iface) {
			continue;
		}

		for (const config of iface) {
			results.add(config.address);
		}
	}

	return results;
};

const checkAvailablePort = (options: net.ListenOptions): Promise<number> => {
	return new Promise((resolve, reject) => {
		const server = net.createServer();
		server.unref();
		server.on('error', reject);

		server.listen(options, () => {
			const address = server.address();
			if (address && typeof address !== 'string') {
				// address is net.AddressInfo
				const { port } = address;
				server.close(() => {
					resolve(port);
				});
			} else {
				// Should not happen once listening is successful,
				// but just in case:
				server.close(() => {
					reject(new Error('Could not get server address'));
				});
			}
		});
	});
};

const getAvailablePort = async (
	options: net.ListenOptions,
	hosts: Set<string | undefined>
): Promise<number> => {
	// If a specific host is given, or port is 0,
	// we just check availability directly.
	if (options.host || options.port === 0) {
		return checkAvailablePort(options);
	}

	// Otherwise, we try to bind to each local host to verify
	// that the port is truly available on all interfaces.
	for (const host of hosts) {
		try {
			await checkAvailablePort({ port: options.port, host });
		} catch (error: unknown) {
			const err = error as NodeJS.ErrnoException;
			// If the error is not "address not available" or "invalid argument",
			// rethrow it because it's a genuine binding error.
			if (!['EADDRNOTAVAIL', 'EINVAL'].includes(err.code ?? '')) {
				throw error;
			}
		}
	}

	// If we get here, the port is valid on all tested hosts.
	return options.port as number;
};

function* portCheckSequence(ports?: Iterable<number>): Generator<number, void, unknown> {
	if (ports) {
		yield* ports;
	}

	// Fallback to port 0, which lets the system pick a random free port
	yield 0;
}

/**
 * Attempts to find an available port, respecting `port`, `exclude`, and `host`
 * options (if provided). If no suitable port is found, rejects with an error.
 */
export default async function getPorts(options?: GetPortsOptions): Promise<number> {
	let ports: Iterable<number> | undefined;
	let exclude = new Set<number>();

	if (options) {
		// Normalize `port` to always be an iterable if defined
		if (options.port !== undefined) {
			if (typeof options.port === 'number') {
				ports = [options.port];
			} else {
				ports = options.port;
			}
		}

		// Validate and store excluded ports
		if (options.exclude) {
			const excludeIterable = options.exclude;
			if (typeof excludeIterable[Symbol.iterator] !== 'function') {
				throw new TypeError('The `exclude` option must be an iterable.');
			}

			for (const element of excludeIterable) {
				if (typeof element !== 'number') {
					throw new TypeError(
						'Each item in the `exclude` option must be a number corresponding to the port to exclude.'
					);
				}

				if (!Number.isSafeInteger(element)) {
					throw new TypeError(
						`Number ${element} in the exclude option is not a safe integer and can't be used`
					);
				}
			}

			exclude = new Set(excludeIterable);
		}
	}

	// Setup the timeout (once) to rotate the lockedPorts sets
	if (timeout === undefined) {
		timeout = setTimeout(() => {
			timeout = undefined;
			lockedPorts.old = lockedPorts.young;
			lockedPorts.young = new Set();
		}, releaseOldLockedPortsIntervalMs);

		// Some environments (e.g. Jest, browsers) may not have `unref`.
		if (typeof timeout.unref === 'function') {
			timeout.unref();
		}
	}

	const hosts = getLocalHosts();

	for (const port of portCheckSequence(ports)) {
		try {
			if (exclude.has(port)) {
				continue;
			}

			// Attempt to get an available port (could be the requested one or random)
			let availablePort = await getAvailablePort({ ...options, port }, hosts);

			// Check whether our "locked" sets include this port
			while (
				lockedPorts.old.has(availablePort) ||
				lockedPorts.young.has(availablePort)
			) {
				if (port !== 0) {
					throw new Locked(port);
				}

				// If port was 0 (meaning "random"), get another random port
				availablePort = await getAvailablePort({ ...options, port }, hosts);
			}

			// Reserve/lock the chosen port
			lockedPorts.young.add(availablePort);

			return availablePort;
		} catch (error: unknown) {
			const err = error as NodeJS.ErrnoException;
			// Rethrow any error that isn't a standard "in use" or "access" error, or our Locked error
			if (!['EADDRINUSE', 'EACCES'].includes(err.code ?? '') && !(error instanceof Locked)) {
				throw error;
			}
		}
	}

	throw new Error('No available ports found');
}

/**
 * Returns an iterator of port numbers starting from `from` to `to` inclusive.
 */
export function portNumbers(from: number, to: number): Generator<number, void, unknown> {
	if (!Number.isInteger(from) || !Number.isInteger(to)) {
		throw new TypeError('`from` and `to` must be integer numbers');
	}

	if (from < minPort || from > maxPort) {
		throw new RangeError(`'from' must be between ${minPort} and ${maxPort}`);
	}

	if (to < minPort || to > maxPort) {
		throw new RangeError(`'to' must be between ${minPort} and ${maxPort}`);
	}

	if (from > to) {
		throw new RangeError('`to` must be greater than or equal to `from`');
	}

	function* generator(start: number, end: number) {
		for (let port = start; port <= end; port++) {
			yield port;
		}
	}

	return generator(from, to);
}

/**
 * Clears both the old and the young locked port sets.
 */
export function clearLockedPorts(): void {
	lockedPorts.old.clear();
	lockedPorts.young.clear();
}