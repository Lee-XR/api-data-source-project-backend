import axios from 'axios';

async function singleFetchSkiddle(reqBody) {
	// const url =
	// 	process.env.NODE_ENV === 'production'
	// 		? process.env.PROD_SKIDDLE_SDK_URL
	// 		: process.env.DEV_SKIDDLE_SDK_URL;

	const url = `https://${process.env.VERCEL_BRANCH_URL}/api/skiddle-api-php`;

	const data = {
		...reqBody,
		access_key: process.env.SKIDDLE_SDK_ACCESS_KEY,
	};

	return await axios.post(url, data).then((response) => {
		const isArray = Array.isArray(response.data.results);

		return {
			totalHits: isArray ? parseInt(response.data.totalcount) : 1,
			records: isArray ? response.data.results : [response.data.results],
		};
	});
}

async function multiFetchSkiddle(totalCount, firstLimit, firstOffset, reqBody) {
	const promises = [];
	const repeats = Math.ceil((totalCount - firstLimit) / 100);

	for (let num = 0; num < repeats; num++) {
		let newOffset = firstLimit + firstOffset + num * 100;
		const paramsOffset = {
			...reqBody.params,
			limit: 100,
			offset: newOffset,
		};

		const promise = new Promise((resolve) => {
			setTimeout(() => {
				resolve(singleFetchSkiddle({ ...reqBody, params: paramsOffset }));
			}, 2000 * num);
		});

		promises.push(promise);
	}

	return Promise.all(promises).then((results) => {
		return results;
	});
}

export async function getSkiddleData(req, res, next) {
	const { params } = req.body;
	const firstLimit = params.limit || 20;
	const firstOffset = params.offset || 0;
	let totalCount = 0;
	let allRecords = [];

	const firstFetch = singleFetchSkiddle(req.body);

	return await firstFetch.then(
		({ totalHits, records }) => {
			totalCount = totalHits;
			allRecords = records;

			if (allRecords.length < totalCount) {
				const multiFetch = multiFetchSkiddle(
					totalCount,
					firstLimit,
					firstOffset,
					req.body
				);

				return multiFetch.then(
					(response) => {
						response.forEach((result) => {
							allRecords.push(...result.records);
						});
					},
					(error) => {
						next(error);
					}
				);
			}

			res.json({totalHits: totalCount, records: allRecords});
		},
		(error) => {
			next(error);	
		}
	);
}
