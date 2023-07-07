import { Transform, pipeline } from 'node:stream';
import { promisify } from 'node:util';
import { createRequire } from 'node:module';
import { parse } from 'csv-parse';
import { stringify } from 'csv-stringify';
import { formatPhoneNumber, removeWhiteSpace } from '../utils/stringUtils.js';

const require = createRequire(import.meta.url);
const allowedApi = require('../assets/mapping-allowed-api.json');
const skiddleFieldMap = require('../assets/fieldmaps/skiddle-venue-fields.json');

// Return API data fields for mapping
async function getApiFieldMap(apiName) {
	switch (apiName) {
		case 'skiddle':
			return skiddleFieldMap;
		case 'noapi':
			return skiddleFieldMap;
		default:
			throw new Error('No API detected. Cannot process data.');
	}
}

// Change field names for new records
async function changeFieldNames(inputDataArray, existingFieldHeaders, apiName) {
	const fieldHeaders = [...existingFieldHeaders];
	const apiFieldMap = await getApiFieldMap(apiName);

	const changedDataArray = inputDataArray.map((record) => {
		const changedRecord = Object.fromEntries(
			Object.entries(record)
				.map(([key, value]) => {
					const newFieldName = apiFieldMap[key];
					if (newFieldName) {
						if (!fieldHeaders.includes(newFieldName)) {
							fieldHeaders.push(newFieldName);
						}

						if (newFieldName === 'venue_pcode') {
							value = removeWhiteSpace(value);
						}

						if (newFieldName === 'venue_phone') {
							value = formatPhoneNumber(value);
						}

						return [newFieldName, value];
					}
					return null;
				})
				.filter((keypair) => keypair !== null)
		);

		return changedRecord;
	});

	return { changedDataArray, fieldHeaders };
}

// Transform stream input to array object
class StreamToObject extends Transform {
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
		} else if (parsedData.inputRecords.length === 0) {
			callback(new Error('No data provided.'));
		} else {
			callback(null, parsedData);
		}
	}
}

// Map records to CSV fields
class MapFields extends Transform {
	constructor(apiName, options) {
		super({ ...options, objectMode: true });
		this.apiName = apiName;
		this.inputDataObject = null;
		this.fieldHeaders = [];
	}

	_transform({ inputRecords, latestCsv }, encoding, callback) {
		this.inputDataObject = inputRecords;
		const csvParser = parse(latestCsv, {
			bom: true,
			to_line: 1,
		});
		csvParser
			.on('readable', () => {
				let row;
				while ((row = csvParser.read()) !== null) {
					this.fieldHeaders.push(...row);
				}
			})
			.on('end', () => {
				(async () => {
					const { changedDataArray, fieldHeaders } = await changeFieldNames(
						this.inputDataObject,
						this.fieldHeaders,
						this.apiName
					);

					callback(null, { changedDataArray, fieldHeaders });
				})().catch((error) => {
					callback(error);
				});
			})
			.on('error', (error) => {
				callback(error);
			});
	}
}

export async function mapFields(req, res, next) {
	const apiName = req.params.apiName.toLowerCase();
	if (!allowedApi.includes(apiName)) {
		return next(new Error('Incorrect API. Not allowed to process data.'));
	}

	const streamToObject = new StreamToObject();
	const mapFields = new MapFields(apiName);

	// Transform object to CSV string
	async function ObjectToCsv(incomingStream) {
		const changedDataArray = [];
		const fieldHeaders = [];

		incomingStream
			.on('readable', function () {
				let row;
				while ((row = this.read()) !== null) {
					changedDataArray.push(...row.changedDataArray);
					fieldHeaders.push(...row.fieldHeaders);
				}
			})
			.on('error', (error) => {
				next(error);
			})
			.on('end', () => {
				stringify(
					changedDataArray,
					{
						objectMode: true,
						header: true,
						columns: fieldHeaders,
					},
					(error, data) => {
						if (error) {
							next(error);
						}

						res.json({ mappedCsv: data, mappedCount: changedDataArray.length });
					}
				);
			});
	}

	const pipe = promisify(pipeline);
	await pipe(req, streamToObject, mapFields, ObjectToCsv).catch((error) => {
		next(error);
	});
}
