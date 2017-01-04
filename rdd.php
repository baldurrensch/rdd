<?php
use Google\Cloud\BigQuery\BigQueryClient;

require __DIR__.'/vendor/autoload.php';

$client = new Google_Client();
$client->useApplicationDefaultCredentials();

$projectId = 'rdd-test';
$bigQuery = new BigQueryClient([
    'projectId' => $projectId,
]);

$timeStart = microtime(true);

// Modified from http://stackoverflow.com/questions/19271381/correctly-determine-if-date-string-is-a-valid-date-in-that-format
function dateIsValid($date)
{
    $d = DateTime::createFromFormat('Y-m-d', $date);
    if (!$d) {
        @list($dateString, $timeString) = explode('T', $date);
        $d = DateTime::createFromFormat('Y-m-d', $dateString);
        $t = DateTime::createFromFormat('H:i:s', substr($timeString, 0, 8));
        if ($t && $t->format('H:i:s') === substr($timeString, 0, 8)) {
            $date = $dateString;
        }
    }

    return $d && $d->format('Y-m-d') === $date;
}

function getPoints($eventType, $payload)
{
    if ($eventType == 'CreateEvent' && $payload->ref_type == 'repository') {
        $earned_points = 10;
    } elseif ($eventType == 'ForkEvent') {
        $earned_points = 5;
    } elseif ($eventType == 'MemberEvent' && $payload->action == 'added') {
        $earned_points = 3;
    } elseif ($eventType == 'PullRequestEvent' && $payload->action == 'closed' && $payload->pull_request->merged) {
        $earned_points = 2;
    } elseif ($eventType == 'WatchEvent' && $payload->action == 'started') {
        $earned_points = 1;
    } elseif ($eventType == 'IssuesEvent' && $payload->action == 'opened') {
        $earned_points = 1;
    } else {
        $earned_points = 0;
    }

    return $earned_points;
}

// Command arguments
$shortOpts = "a:b:t:";
$longOpts = [
    "after:",
    "before:",
    "top:",
];
$options = getopt($shortOpts, $longOpts);
extract($options);

// $endDate cannot be later than today
if (!empty($before) && dateIsValid($before) && strtotime($before) < time()) {
    $endDT = new DateTime($before);
} else {
    $endDT = new DateTime();
}
$endDate = $endDT->format('Y-m-d');

// $startDate cannot be later than today
if (!empty($after) && dateIsValid($after) && strtotime($after) < time()) {
    $startDT = new DateTime($after);
} else {
    $startDT = new DateTime();
    $startDT->modify('-28 day');
}
$startDate = $startDT->format('Y-m-d');

// $startDate cannot be later than $endDate
if ($endDT->getTimestamp() < $startDT->getTimestamp()) {
    $endDate = date('Y-m-d');
}
// validate $top argument to be an integer
$topNum = (!empty($top) && is_numeric($top)) ? (int) $top : 20;

$results = [];

// Calculate the day between start and end dates. On the first and last date, stay within the specified hours
$interval = $startDT->diff($endDT);
$days = $interval->days;
for ($i = 0; $i < $days; $i++) {
    $dt = new DateTime($startDate);
    $dt->modify("+{$i} day");
    $calcDate = $dt->format('Ymd');
    $first = ($i == 0) ? true : false;
    $last = ($i == ($days - 1)) ? true : false;
    if ($first) {
        $startRange = $startDT->format('Y-m-d H:i:s');
        $endRange = $dt->format('Y-m-d 23:59:59');
    } elseif ($last) {
        $startRange = $dt->format('Y-m-d 00:00:00');
        $endRange = $endDT->format('Y-m-d H:i:s');
    } else {
        $startRange = $dt->format('Y-m-d 00:00:00');
        $endRange = $dt->format('Y-m-d 23:59:59');
    }
    $table = ($first || $last) ? "(TABLE_DATE_RANGE([githubarchive:day.{$calcDate}], TIMESTAMP({$startRange}), TIMESTAMP({$endRange})))" : "[githubarchive:day.{$calcDate}]";

    $query = <<<SQL
SELECT 
  type, 
  repo.name, 
  actor.login,
  payload, 
FROM 
  {$table}
WHERE 
  type = 'CreateEvent'
OR 
  type = 'ForkEvent'
OR
  type = 'MemberEvent'
OR
  type = 'PullRequestEvent'
OR
  type = 'WatchEvent'
OR
  type = 'IssuesEvent'
SQL;
    $options = ['useLegacySql' => true];
    $queryResults = $bigQuery->runQuery($query, $options);
    if ($queryResults->isComplete()) {
        $rows = $queryResults->rows();
        foreach ($rows as $row) {
            $event = [
                'type'        => '',
                'repo_name'   => '',
                'actor_login' => '',
                'payload'     => '',
            ];
            foreach ($row as $column => $value) {
                $event[$column] = $value;
            }
            $jsonParam = json_decode($event['payload']);
            $points = getPoints($event['type'], $jsonParam);
            if ($points > 0) {
                @$results[$event['repo_name']] += $points;
            }
        }
    } else {
        throw new Exception('The query failed to complete');
    }
}

arsort($results);

$timeEnd = microtime(true);
$time = round(($timeEnd - $timeStart), 3);

echo "Getting Github statistics for {$startDT->format('Y-m-d H:i:s')} UTC - {$endDT->format('Y-m-d H:i:s')} UTC\n";
echo "Results (~{$time} seconds):\n";
$i = 1;
if (!empty($results)) {
    foreach ($results as $repoName => $points) {
        echo "#{$i}. $repoName - $points ";
        echo ($points == 1) ? 'point' : 'points';
        echo "\n";
        if ($i == $topNum) {
            break;
        }
        $i++;
    }
}
