import express from 'express';
import { matchFields } from '../controllers/matchingController';

export const router = express.Router();

router.post('/:apiName', matchFields);