import dotenv from 'dotenv';
import express from 'express';
import cors from 'cors';

import { router as apiRoutes } from './routes/apiRoutes.js';
import { returnError } from './middleware/errorHandler.js';

dotenv.config();
const app = express();
const port = process.env.PORT || 3000;
app.use(express.json());

// cors configuration
const corsOptions = {
	origin:
		process.env.NODE_ENV === 'production'
			? process.env.ORIGIN_URL_PROD
			: process.env.ORIGIN_URL_DEV,
	methods: ['GET', 'POST'],
};
app.use(cors(corsOptions));

app.get('/', (req, res) => {
	res.json({msg: 'Hello World'});
});

// routes
app.use('/api', apiRoutes);

// error handler middleware
app.use(returnError);

app.listen(port, () => {
	console.log(`Server listening on port ${port}`);
});
