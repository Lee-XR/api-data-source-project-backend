<?php
// phpinfo();

require_once(__DIR__ . '/../vendor/autoload.php');

/* Disable during production */
// $dotenv = Dotenv\Dotenv::createImmutable(__DIR__ . '/');
// $dotenv->load();

$origin = $_ENV['NODE_ENV'] === 'production' ? $_ENV['PROD_ORIGIN_URL'] : $_ENV['DEV_ORIGIN_URL'];

// Prevent direct access
if ($_SERVER['HTTP_ORIGIN'] !== $origin) {
    http_response_code(401);
    $error = ['error' => 'Direct access not allowed.'];
    $response = json_encode($error);
    echo $response;
    exit();
}

header('Access-Control-Allow-Origin: ' . $origin);
header('Access-Control-Allow-Methods: POST');
header('Access-Control-Allow-Headers: Content-Type, x-requested-with');

try {
    // Get JSON body data from POST request
    $requestBody = json_decode(file_get_contents('php://input'), true);
    if (empty($requestBody)) {
        throw new Exception('Request body is empty.');
    }

    $type = isset($requestBody['type']) ? $requestBody['type'] : null;
    $id = isset($requestBody['id']) ? $requestBody['id'] : null;
    $params = isset($requestBody['params']) ? $requestBody['params'] : null;
    if (empty($type)) {
        throw new Exception('No search type is provided.');
    }

    // Validate if API key exists
    $api_key = $_ENV['SKIDDLE_API_KEY'];
    if (empty($api_key)) {
        throw new Exception('No API key found.');
    }

    // Authenticate using Skiddle API key
    try {
        $session = new SkiddleSDK\SkiddleSession(['api_key' => $api_key]);
    } catch (SkiddleSDK\SkiddleException $e) {
        throw new Exception($e->getMessage());
    }

    // Initialise type class
    $className = 'SkiddleSDK\\' . ucfirst($type);
    $class = new $className;
    try {
        $class->setSession($session);
    } catch (SkiddleSDK\SkiddleException $e) {
        throw new Exception($e->getMessage());
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
        throw new Exception($e->getMessage());
    }

} catch(Exception $e) {
    $error = ['error' => $e->getMessage()];
    $response = json_encode($error);

    if ($_SERVER['REQUEST_METHOD'] === 'POST') {
        http_response_code(500);
    }
}

echo $response;