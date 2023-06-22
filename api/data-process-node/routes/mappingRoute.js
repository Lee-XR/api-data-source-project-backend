import express from 'express';
import { mapFields } from '../controllers/mappingController.js';

export const router = express.Router();

router.post('/:apiName', mapFields);
router.get('/test', (req, res) => {
    res.json({test: 'Test Message'});
})