import { Transform, pipeline } from 'node:stream';
import { promisify } from 'node:util';
import { createRequire } from 'node:module';
import { parse } from 'csv-parse';
import { stringify } from 'csv-stringify';
import {
	removeAllSymbols,
	removeWhiteSpace,
	filterWordsFromString,
} from '../utils/stringUtils.js';

const require = createRequire(import.meta.url);
const allowedApi = require('../assets/apiAllowMatching.json');

// Transform CSV string to object
async function csvStringToObj(string, options) {
	const records = [];
	const csvParser = parse(string, {
		...options,
		bom: true,
		columns: true,
	});

	for await (const record of csvParser) {
		records.push(record);
	}

	return records;
}

// Transform object to CSV string
async function objToCsvString(object, options) {
	const stringifyOptions = {
		objectMode: true,
		header: true,
		...options,
	};

	const stringifier = stringify(object, stringifyOptions);
	let csvString = '';
	for await (const row of stringifier) {
		csvString += row;
	}

	return csvString;
}

// Transform stream input to CSV string
class StreamToObj extends Transform {
	constructor(options) {
		super({ ...options, objectMode: true });
		this.dataString = '';
	}

	_transform(chunk, encoding, callback) {
		this.dataString += chunk;
		callback();
	}

	_flush(callback) {
		const parsedData = JSON.parse(this.dataString);
		if (parsedData.latestCsv.length === 0) {
			callback(new Error('Latest CSV data not provided.'));
		}
		if (parsedData.mappedCsv.length === 0) {
			callback(new Error('No data provided.'));
		}

		csvStringToObj(parsedData.latestCsv)
			.then((latestCsv) => {
				callback(null, { latestCsv, mappedCsv: parsedData.mappedCsv });
			})
			.catch((error) => {
				callback(error);
			});
	}
}

// Filter latest and mapped CSV data to separate pipes
class FilterCsvType extends Transform {
	constructor(type, options) {
		super({ ...options, objectMode: true });
		this.type = type;
		this.csvRecords;
		this.isError = false;
	}

	_transform(chunk, encoding, callback) {
		switch (this.type) {
			case 'latest':
				this.csvRecords = chunk.latestCsv;
				break;

			case 'mapped':
				this.csvRecords = chunk.mappedCsv;
				break;

			default:
				this.isError = true;
				break;
		}

		if (this.isError) {
			callback(new Error('Invalid CSV type provided.'));
		} else {
			callback(null, this.csvRecords);
		}
	}
}

export async function matchRecords(req, res, next) {
	const apiName = req.params.apiName.toLowerCase();
	if (!allowedApi.includes(apiName)) {
		return next(new Error('Incorrect API. Not allowed to process data.'));
	}

	const streamToObj = new StreamToObj();
	const latestCsvFilter = new FilterCsvType('latest');
	const mappedCsvFilter = new FilterCsvType('mapped');
	const csvParser = parse({
		bom: true,
		columns: true,
	});

	async function processOutput(incomingStream) {
		const existingRecords = [];
		const recordsZeroMatch = [];
		const recordsHasMatch = [];

		incomingStream
			.on('readable', function () {
				let record;
				while ((record = this.read()) !== null) {
					existingRecords.push(...record);
				}
			})
			.on('error', (error) => {
				next(error);
			})
			.on('end', async () => {
				for await (let record of csvParser) {
					await compareFields(record, existingRecords)
						.then(({ matchedFields, matchedFieldsNum }) => {
							record = {
								...record,
								...matchedFields,
							};

							if (matchedFieldsNum > 0) {
								recordsHasMatch.push(record);
							} else {
								recordsZeroMatch.push(record);
							}
						})
						.catch((error) => {
							next(error);
						});
				}

				const zeroMatchCount = recordsZeroMatch.length;
				const hasMatchCount = recordsHasMatch.length;

				Promise.all([
					objToCsvString(recordsZeroMatch),
					objToCsvString(recordsHasMatch),
				])
					.then(([zeroMatchCsv, hasMatchCsv]) => {
						res.json({
							zeroMatchCsv,
							zeroMatchCount,
							hasMatchCsv,
							hasMatchCount,
						});
					})
					.catch((error) => {
						next(error);
					});
			});
	}

	const pipe = promisify(pipeline);
	await pipe(req, streamToObj)
		.then(() => {
			pipeline(streamToObj, latestCsvFilter, processOutput, (error) => {
				if (error) {
					next(error);
				}
			});
			pipeline(streamToObj, mappedCsvFilter, csvParser, (error) => {
				if (error) {
					next(error);
				}
			});
		})
		.catch((error) => {
			next(error);
		});
}
