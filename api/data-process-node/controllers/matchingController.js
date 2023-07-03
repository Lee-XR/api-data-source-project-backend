import { PassThrough, Transform, pipeline } from 'node:stream';
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
	const csvDataPassThrough = new PassThrough({ objectMode: true });
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
	await pipe(req, streamToObj, csvDataPassThrough).catch((error) => next(error));
	pipe(csvDataPassThrough, mappedCsvFilter, csvParser).catch((error) => next(error));
	pipe(csvDataPassThrough, latestCsvFilter, processOutput).catch((error) => next(error));
	// pipeline(req, streamToObj, csvDataPassThrough, (error) => {
	// 	if (error) {
	// 		next(error);
	// 	} else {
	// 		pipe(csvDataPassThrough, mappedCsvFilter, csvParser).catch((error) => next(error));
	// 		pipe(csvDataPassThrough, latestCsvFilter, processOutput).catch((error) => next(error));
	// 	}
	// });
}
