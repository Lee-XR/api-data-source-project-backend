import express from 'express';
import { matchRecords } from '../controllers/matchingController.js';

export const router = express.Router();

router.post('/:apiName', matchRecords);