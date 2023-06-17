<?php
// phpinfo();

require_once __DIR__ . '/../../vendor/autoload.php';
require_once __DIR__ . '/configs/dotenvConfig.php';

$origin = $_ENV['PHP_ENV'] === 'production'
            ? $_ENV['ORIGIN_URL_PROD']
            : $_ENV['ORIGIN_URL_DEV'];

header('Access-Control-Allow-Origin: ' . $origin);
header('Access-Control-Allow-Methods: OPTIONS, POST');
header('Access-Control-Allow-Headers: X-CSRF-Token, X-Requested-With, Accept, Accept-Version, Content-Length, Content-MD5, Content-Type, Date, X-Api-Version');

// Allow CORS preflight request
if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
    http_response_code(200);
    echo json_encode(['body' => 'OK']);
    exit();
}

try {
    // Get JSON body data from POST request
    $requestBody = json_decode(file_get_contents('php://input'), true);
    if (empty($requestBody)) {
        throw new Exception('Request body is empty.', 500);
    }

    $type = isset($requestBody['type']) ? $requestBody['type'] : null;
    $id = isset($requestBody['id']) ? $requestBody['id'] : null;
    $params = isset($requestBody['params']) ? $requestBody['params'] : null;
    if (empty($type)) {
        throw new Exception('No search type is provided.', 500);
    }

    // Validate if API key exists
    $api_key = $_ENV['SKIDDLE_API_KEY'];
    if (empty($api_key)) {
        throw new Exception('No API key found.', 500);
    }

    // Authenticate using Skiddle API key
    try {
        $session = new SkiddleSDK\SkiddleSession(['api_key' => $api_key]);
    } catch (SkiddleSDK\SkiddleException $e) {
        throw new Exception($e->getMessage(), 500);
    }

    // Initialise type class
    $className = 'SkiddleSDK\\' . ucfirst($type);
    $class = new $className;
    try {
        $class->setSession($session);
    } catch (SkiddleSDK\SkiddleException $e) {
        throw new Exception($e->getMessage(), 500);
    }

    // Return API results
    try {
        // Set parameters
        if (isset($params)) {
            foreach($params as $key => $value) {
                $class->addCond($key, $value);
            }
        }
        $data = isset($id) ? $class->getListing($id) : $class->getListings();
        $response = json_encode($data);

    } catch (SkiddleSDK\SkiddleException $e) {
        throw new Exception($e->getMessage(), 500);
    }

} catch(Exception $e) {
    $error = ['error' => $e->getMessage()];
    $response = json_encode($error);
    http_response_code($e->getCode());
}

echo $response;