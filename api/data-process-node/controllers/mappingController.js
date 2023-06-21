import { Transform, finished, pipeline } from 'node:stream';
import { promisify } from 'node:util';
import { createRequire } from 'node:module';
import { parse } from 'csv-parse';
import { stringify } from 'csv-stringify';
import { createReadStream } from 'node:fs';

const require = createRequire(import.meta.url);
const testFields = require('../assets/fieldmaps/test-fields.json');
const skiddleFields = require('../assets/fieldmaps/Skiddle-Venue-Fields.json');

// Return API data fields
function getApiFields(apiName) {
	switch(apiName) {
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
					if (apiFields[key]) {
						if (!fieldHeaders.includes(apiFields[key])) {
							fieldHeaders.push(apiFields[key]);
						}
						return [apiFields[key], value];
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

// Transform records to CSV
class MapFields extends Transform {
	constructor(apiName) {
		super({ objectMode: true });
		this.apiName = apiName;
	}

	_transform(chunk, encoding, callback) {
		const fields = [];
		const stream = createReadStream(
			'api/data-process-node/assets/Liverpool_090623.csv'
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
		stringify(newArray, {
			...this.options,
			header: true,
			columns: fieldHeaders
		}, (error, data) => {
			if (error) {
				console.log(error);
				callback(error);
			} else {
				callback(null, data);
			}
		});
	}
}

async function mapFields(req, res, next) {
	const streamToObj = new StreamToObj();
	const mapFields = new MapFields(req.params.apiName);
	const objToCsv = new ObjToCsv();

	try {
		const pipe = promisify(pipeline);
		await pipe(req, streamToObj, mapFields, objToCsv, res);
	} catch (error) {
		console.log(error);
		next(error);
	}
}

export { mapFields };
