import { createReadStream } from 'node:fs';
import { Transform, pipeline } from 'node:stream';
import { createRequire } from 'node:module';
import { promisify } from 'node:util';
import { finished } from 'node:stream/promises';
import path from 'node:path';
import { parse } from 'csv-parse';
import { removeAllSymbols, removeWhiteSpace } from '../utils/stringUtils.js';

const require = createRequire(import.meta.url);
const allowedApi = require('../assets/allowedApi.json');

const testRecord = {
	id: 115852,
	venue_name: 'Phase One Jacaranda',
	address: '40 Seel Street',
	venue_city: 'Liverpool',
	venue_pcode: 'L14BE',
	venue_phone: '01513631292',
	description: '',
	imageurl: '',
	latitude: 53.4027351,
	longitude: -2.9813483,
	distance: 0.791196,
	type: 'Nightclub',
};

// Transform stream input to CSV string
class StreamToString extends Transform {
	constructor(options) {
		super({ ...options, objectMode: true });
		this.csvString = '';
	}

	_transform(chunk, encoding, callback) {
		this.csvString += chunk;
		callback();
	}

	_flush(callback) {
		this.push(this.csvString);
		callback();
	}
}

// Get existing CSV records
async function getExistingRecords() {
	const records = [];
	const existingCsvStream = createReadStream(
		path.resolve(
			process.cwd(),
			'api',
			'data-process-node',
			'assets',
			'Liverpool_090623.csv'
		)
	);

	existingCsvStream
		.pipe(parse({ columns: true }))
		.on('data', (record) => {
			records.push(record);
		})
		.on('error', (error) => {
			console.log(error);
			next(error);
		});

	await finished(existingCsvStream);
	return records;
}

// Filter existing records with similar venue names
async function matchVenueName(name, existingRecords) {
	const nameKeywords = removeAllSymbols(name).toLowerCase().split(' ');

	const records = await existingRecords
		.filter((record) => {
			const venueName = removeAllSymbols(record.venue_name)
				.toLowerCase()
				.split(' ');

			for (const keyword of nameKeywords) {
				for (const name of venueName) {
					if (name === keyword) {
						return true;
					}
				}
			}
			return false;
		})
		.map((record) => ({ ...record, matched_fields: 'venue_name' }));

	return records;
}

// Filter matched records with same venue city
async function matchVenueCity(city, matchedRecords) {
	const cityKeywords = removeAllSymbols(city).toLowerCase().split(' ');

	const records = await matchedRecords.map((record) => {
		const venueCity = removeAllSymbols(record.venue_city).toLowerCase();

		for (const keyword of cityKeywords) {
			if (venueCity.includes(cityKeywords)) {
				return {
					...record,
					matched_fields: `${record.matched_fields}, venue_city`,
				};
			}
		}
		return record;
	});

	return records;
}

// Filter matched records with same postcode
async function matchVenuePostcode(postcode, matchedRecords) {
	const postcodeUpperCased = postcode.toUpperCase();

	const records = await matchedRecords.map((record) => {
		if (record.venue_pcode.toUpperCase() === postcodeUpperCased) {
			return {
				...record,
				matched_fields: `${record.matched_fields}, venue_pcode`,
			};
		}
		return record;
	});

	return records;
}

// Filter matched records with same last 7 phone number characters
async function matchVenuePhone(phone, matchedRecords) {
	const phoneLastSevenChars = removeWhiteSpace(phone).substring(
		phone.length - 7,
		phone.length
	);

	const records = await matchedRecords.map((record) => {
		const venuePhone = removeWhiteSpace(record.venue_phone);
		const venuePhoneLastSevenChars = venuePhone.substring(
			venuePhone.length - 7,
			venuePhone.length
		);

		if (phoneLastSevenChars === venuePhoneLastSevenChars) {
			return {
				...record,
				matched_fields: `${record.matched_fields}, venue_phone`,
			};
		}

		return record;
	});

	return records;
}

// Compare record fields based on matrix
async function compareFields(record, existingRecords) {
	const name = record.venue_name;
	const city = record.venue_city;
	const postcode = record.venue_pcode;
	const phone = record.venue_phone;

	const matchedRecords = [];
	const matchedByVenueName = await matchVenueName(name, existingRecords);
	const matchedByVenueCity = await matchVenueCity(city, matchedByVenueName);
	const matchedByVenuePostcode = await matchVenuePostcode(postcode, matchedByVenueCity);
	const matchedByVenuePhone = await matchVenuePhone(phone, matchedByVenuePostcode);

	matchedRecords.push(...matchedByVenuePhone);

	// for (const record of matchedRecords) {
	// 	console.log(record.venue_name, record.matched_fields);
	// }
}

export async function matchRecords(req, res, next) {
	const apiName = req.params.apiName.toLowerCase();
	if (!allowedApi.includes(apiName)) {
		next(new Error('Incorrect API. Not allowed to process data.'));
	}

	const existingRecords = await getExistingRecords();
	const streamToString = new StreamToString();

	// Initialise CSV parser & invoke comparison function
	const csvParser = parse({
		columns: true,
	});
	// (async function () {
	// 	for await (const record of csvParser) {
	// 		compareFields(record, existingRecords);
	// 	}
	// })();
	if (existingRecords) {
		compareFields(testRecord, existingRecords);
	}

	const pipe = promisify(pipeline);
	await pipe(req, streamToString, csvParser).catch((error) => {
		console.log(error);
		next(error);
	});

	res.json({ success: true });
}
