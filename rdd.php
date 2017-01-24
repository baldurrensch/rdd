#!/usr/bin/env php
<?php
require __DIR__ . '/vendor/autoload.php';

use Jsoh\Console\Command\RddCommand;
use Symfony\Component\Console\Application;

$command = new RddCommand();
$app = new Application('GitHub Repo Rating App', '0.1');
$app->add($command);
$app->setDefaultCommand($command->getName());
$app->run();
