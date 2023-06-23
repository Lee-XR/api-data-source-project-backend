import path from 'node:path';
import { createReadStream } from 'node:fs';
import { Transform, pipeline } from 'node:stream';
import { promisify } from 'node:util';
import { createRequire } from 'node:module';
import { parse } from 'csv-parse';
import { stringify } from 'csv-stringify';

import { formatPhoneNumber, removeWhiteSpace } from '../utils/stringUtils.js';

const require = createRequire(import.meta.url);
const testFields = require('../assets/fieldmaps/test-fields.json');
const skiddleFields = require('../assets/fieldmaps/Skiddle-Venue-Fields.json');

// Return API data fields for mapping
function getApiFields(apiName) {
	switch (apiName) {
		case 'skiddle':
			return skiddleFields;

		default:
			return testFields;
	}
}

// Change field names for new records
function changeFieldNames(dataArray, existingFields, apiName) {
	const fieldHeaders = [...existingFields];
	const apiFields = getApiFields(apiName);

	const newArray = dataArray.map((obj) => {
		const newObj = Object.fromEntries(
			Object.entries(obj)
				.map(([key, value]) => {
					const newFieldname = apiFields[key];
					if (newFieldname) {
						if (!fieldHeaders.includes(newFieldname)) {
							fieldHeaders.push(newFieldname);
						}

						if (newFieldname === 'venue_pcode') {
							value = removeWhiteSpace(value);
						}

						if (newFieldname === 'venue_phone') {
							value = formatPhoneNumber(value);
						}

						return [newFieldname, value];
					}
					return null;
				})
				.filter((pair) => pair !== null)
		);
		return { ...newObj };
	});

	return { newArray, fieldHeaders };
}

// Transform stream input to array object
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
		this.push(JSON.parse(this.dataString));
		callback();
	}
}

// Map records to CSV fields
class MapFields extends Transform {
	constructor(apiName) {
		super({ objectMode: true });
		this.apiName = apiName;
	}

	_transform(chunk, encoding, callback) {
		const fields = [];
		const stream = createReadStream(
			path.resolve(process.cwd(), 'api', 'data-process-node', 'assets', 'Liverpool_090623.csv')
		);

		stream
			.pipe(parse({ toLine: 1 }))
			.on('data', (record) => {
				fields.push(...record);
			})
			.on('end', () => {
				const { newArray, fieldHeaders } = changeFieldNames(
					chunk,
					fields,
					this.apiName
				);
				this.push({ newArray, fieldHeaders });
				callback();
			})
			.on('error', (error) => {
				console.log(error);
				callback(error);
			});
	}
}

// Transform object to CSV string
class ObjToCsv extends Transform {
	constructor(options) {
		super({ ...options, objectMode: true });
		this.options = { ...options, objectMode: true };
	}

	_transform({ newArray, fieldHeaders }, encoding, callback) {
		stringify(
			newArray,
			{
				...this.options,
				header: true,
				columns: fieldHeaders,
			},
			(error, data) => {
				if (error) {
					console.log(error);
					callback(error);
				} else {
					this.push(data);
					callback();
				}
			}
		);
	}
}

const allowedApi = ['skiddle'];

export async function mapFields(req, res, next) {
	if (!allowedApi.includes(req.params.apiName)) {
		next(new Error('Incorrect API. Not allowed to process data.'));
	}

	const streamToObj = new StreamToObj();
	const mapFields = new MapFields(req.params.apiName);
	const objToCsv = new ObjToCsv();

	const pipe = promisify(pipeline);
	await pipe(req, streamToObj, mapFields, objToCsv, res)
		.catch((error) => {
			console.log(error);
			next(error);
		});
}
