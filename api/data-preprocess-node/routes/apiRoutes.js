import { Router } from 'express';
import { getSkiddleData } from '../controllers/skiddleController.js';

export const router = Router();

// API routes
router.post('/skiddle', getSkiddleData);
