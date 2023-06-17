<?php

/* Disable $dotenv during production */
if (!getenv('PHP_ENV')) {
    $dotenv = Dotenv\Dotenv::createImmutable(__DIR__ . '/../../../');
    $dotenv->load();
}