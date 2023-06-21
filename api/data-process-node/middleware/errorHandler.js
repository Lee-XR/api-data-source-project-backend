export function returnError(error, req, res, next) {
    if (error.response) {
        res.status(error.response.status).json(error.response.data);
    }
    res.status(500).json({ error: error.message });
}