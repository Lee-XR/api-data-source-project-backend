import path from 'node:path';
import { createReadStream } from 'node:fs';
import { Transform, pipeline } from 'node:stream';
import { promisify } from 'node:util';
import { finished } from 'node:stream/promises';
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
		if (this.csvString.length === 0) {
			callback(new Error('No data provided.'));
		}
		callback(null, this.csvString);
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
		.pipe(parse({ bom: true, columns: true }))
		.on('data', (record) => {
			records.push(record);
		})
		.on('error', (error) => {
			throw new Error(error);
		});

	await finished(existingCsvStream);
	return records;
}

// Filter existing records with similar venue names
async function matchVenueName(name, existingRecords) {
	const nameLowerCased = removeAllSymbols(name).toLowerCase();
	const filterWords = ['the', 'and', '&'];
	const nameKeywords = filterWordsFromString(nameLowerCased, filterWords).split(
		' '
	);
	const matchedRecordsId = [];

	const records = await existingRecords.filter((record) => {
		const venueName = removeAllSymbols(record.venue_name)
			.toLowerCase()
			.split(' ');

		for (const keyword of nameKeywords) {
			if (keyword !== '') {
				for (const name of venueName) {
					if (name === keyword) {
						matchedRecordsId.push(record.id);
						return true;
					}
				}
			}
		}
		return false;
	});

	return { matchedByVenueName: records, venueNameMatches: matchedRecordsId };
}

// Filter matched records with same venue city
async function matchVenueCity(city, matchedRecords) {
	const cityKeywords = removeAllSymbols(city).toLowerCase().split(' ');
	const matchedRecordsId = [];

	const records = await matchedRecords.map((record) => {
		const venueCity = removeAllSymbols(record.venue_city).toLowerCase();

		for (const keyword of cityKeywords) {
			if (venueCity.includes(cityKeywords)) {
				matchedRecordsId.push(record.id);
			}
		}
		return record;
	});

	return { matchedByVenueCity: records, venueCityMatches: matchedRecordsId };
}

// Filter matched records with same postcode
async function matchVenuePostcode(postcode, matchedRecords) {
	const postcodeUpperCased = postcode.toUpperCase();
	const matchedRecordsId = [];

	const records = await matchedRecords.map((record) => {
		if (record.venue_pcode.toUpperCase() === postcodeUpperCased) {
			matchedRecordsId.push(record.id);
		}
		return record;
	});

	return {
		matchedByVenuePostcode: records,
		venuePostcodeMatches: matchedRecordsId,
	};
}

// Filter matched records with same last 7 phone number characters
async function matchVenuePhone(phone, matchedRecords) {
	const recordPhone = removeWhiteSpace(phone);
	const phoneLastSevenChars = recordPhone.substring(recordPhone.length - 7);
	const matchedRecordsId = [];

	const records = await matchedRecords.map((record) => {
		const venuePhone = removeWhiteSpace(record.venue_phone);
		const venuePhoneLastSevenChars = venuePhone.substring(
			venuePhone.length - 7,
			venuePhone.length
		);

		if (phoneLastSevenChars === venuePhoneLastSevenChars) {
			matchedRecordsId.push(record.id);
		}

		return record;
	});

	return { matchedByVenuePhone: records, venuePhoneMatches: matchedRecordsId };
}

// Compare record fields based on selected fields
async function compareFields(record, existingRecords) {
	const name = record.venue_name;
	const city = record.venue_city;
	const postcode = record.venue_pcode;
	const phone = record.venue_phone;

	const { matchedByVenueName, venueNameMatches } = await matchVenueName(
		name,
		existingRecords
	);
	const { matchedByVenueCity, venueCityMatches } = await matchVenueCity(
		city,
		matchedByVenueName
	);
	const { matchedByVenuePostcode, venuePostcodeMatches } =
		await matchVenuePostcode(postcode, matchedByVenueCity);
	const { matchedByVenuePhone, venuePhoneMatches } = await matchVenuePhone(
		phone,
		matchedByVenuePostcode
	);

	const matchedFields = {
		matched_venue_name: venueNameMatches,
		matched_venue_city: venueCityMatches,
		matched_venue_postcode: venuePostcodeMatches,
		matched_venue_phone: venuePhoneMatches,
	};

	let matchedFieldsNum = 0;
	for (const field in matchedFields) {
		if (matchedFields[field].length > 0) {
			matchedFieldsNum++;
		}
		matchedFields[field] = matchedFields[field].toString();
	}

	return { matchedFields, matchedFieldsNum };
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

export async function matchRecords(req, res, next) {
	const apiName = req.params.apiName.toLowerCase();
	if (!allowedApi.includes(apiName)) {
		next(new Error('Incorrect API. Not allowed to process data.'));
	}

	const streamToString = new StreamToString();
	const csvParser = parse({
		bom: true,
		columns: true,
	});

	await getExistingRecords()
		.then((existingRecords) => {
			(async function () {
				const recordsZeroMatch = [];
				const recordsHasMatch = [];

				for await (let record of csvParser) {
					const { matchedFields, matchedFieldsNum } = await compareFields(
						record,
						existingRecords
					);

					record = {
						...record,
						...matchedFields,
					};

					if (matchedFieldsNum > 0) {
						recordsHasMatch.push(record);
					} else {
						recordsZeroMatch.push(record);
					}
				}

				return { recordsZeroMatch, recordsHasMatch };
			})()
				.then(({ recordsZeroMatch, recordsHasMatch }) => {
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
							throw new Error(error);
						});
				})
				.catch((error) => {
					throw new Error(error);
				});
		})
		.catch((error) => {
			next(error);
		});

	const pipe = promisify(pipeline);
	await pipe(req, streamToString, csvParser).catch((error) => {
		next(error);
	});
}
