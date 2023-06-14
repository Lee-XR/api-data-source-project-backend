require('dotenv').config();
const express = require('express');
const cors = require('cors');

const app = express();
const port = process.env.DATA_PREPROCESS_NODE_PORT || 3000;
app.use(express.json());

// cors configuration
const corsOptions = {
	origin:
		process.env.NODE_ENV === 'production'
			? process.env.PROD_ORIGIN_URL
			: process.env.DEV_ORIGIN_URL,
	methods: ['GET', 'POST'],
};
app.use(cors(corsOptions));

// routes
app.get('/', (req, res) => {
	res.json({ msg: 'Hello World' });
});

app.post('/data', (req, res) => {
	const { data } = req.body;
    
	res.json({ msg: 'Request body POST', data });
});

app.listen(port, () => {
	console.log(`Connected to data-preprocess-node backend at port ${port}`);
});
