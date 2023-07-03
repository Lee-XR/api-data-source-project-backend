import path from 'node:path';
import { createWriteStream } from 'node:fs';
import { pipeline } from 'node:stream';
import { promisify } from 'node:util';

export async function updateCsv(req, res, next) {
    // const filePath = path.resolve(process.cwd(), 'api', 'data-process-node', 'tmp', 'VenueRecordsData.csv');
    const fileDestination = createWriteStream('/tmp/VenueRecordsData.csv');
    
    const pipe = promisify(pipeline);
    await pipe(req, fileDestination)
        .then(() => {
            res.json({isSuccess: true});
        })
        .catch((error) => {
            next(error);
        })
}