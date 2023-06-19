import { Transform, pipeline } from 'node:stream';

class StreamToObjArray extends Transform {
    constructor(options) {
        super({...options, readableObjectMode: true, writableObjectMode: true});
    }

    _transform(chunk, encoding, callback) {
        callback(null, chunk);
    }
}

async function mapFields(req, res) {
    const streamToObjArray = new StreamToObjArray();

    req.pipe(streamToObjArray)
        .on('error', (error) => res.status(500).json({error: error.message}))
        .pipe(res);
}

export { mapFields }