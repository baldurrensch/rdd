<?php

namespace Jsoh\Command;

use Google\Cloud\BigQuery\BigQueryClient;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;


class RddCommand extends Command
{
    protected function configure()
    {
        $this
            ->setName('rddrun')
            ->setDescription('RDD App')
            ->setDefinition(
                new InputDefinition([
                    new InputArgument('after', 'a', InputArgument::OPTIONAL),
                    new InputArgument('before', 'b', InputArgument::OPTIONAL),
                    new InputArgument('top', 't', InputArgument::OPTIONAL),
                ])
            );
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        // Modified from http://stackoverflow.com/questions/19271381/correctly-determine-if-date-string-is-a-valid-date-in-that-format
        function dateIsValid($date)
        {
            $d = \DateTime::createFromFormat('Y-m-d', $date);
            if (!$d) {
                @list($dateString, $timeString) = explode('T', $date);
                $d = \DateTime::createFromFormat('Y-m-d', $dateString);
                $t = \DateTime::createFromFormat('H:i:s', substr($timeString, 0, 8));
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

        $timeStart = microtime(true);

        // $client = new Google_Client();
        // $client->useApplicationDefaultCredentials();

        $projectId = 'rdd-test';
        $bigQuery = new BigQueryClient([
            'projectId' => $projectId,
        ]);

        $before = $input->getArgument('before');
        $after = $input->getArgument('after');
        $top = $input->getArgument('top');

        // $endDate cannot be later than today
        if (!empty($before) && dateIsValid($before) && strtotime($before) < time()) {
            $endDT = new \DateTime($before);
        } else {
            $endDT = new \DateTime();
        }
        $endDate = $endDT->format('Y-m-d');

        // $startDate cannot be later than today
        if (!empty($after) && dateIsValid($after) && strtotime($after) < time()) {
            $startDT = new \DateTime($after);
        } else {
            $startDT = new \DateTime();
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
            $dt = new \DateTime($startDate);
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
            $table = ($first || $last) ? "(TABLE_DATE_RANGE([githubarchive:day.], TIMESTAMP('{$startRange}'), TIMESTAMP('{$endRange}') ) )" : "[githubarchive:day.{$calcDate}]";

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
            /*
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
            */
        }

        arsort($results);

        $timeEnd = microtime(true);
        $time = round(($timeEnd - $timeStart), 3);

        $message = '';
        $message .= "Getting Github statistics for {$startDT->format('Y-m-d H:i:s')} UTC - {$endDT->format('Y-m-d H:i:s')} UTC\n";
        $message .= "Results (~{$time} seconds):\n";
        $i = 1;
        if (!empty($results)) {
            foreach ($results as $repoName => $points) {
                $message .= "#{$i}. $repoName - $points ";
                $message .= ($points == 1) ? 'point' : 'points';
                $message .= "\n";
                if ($i == $topNum) {
                    break;
                }
                $i++;
            }
        }

        $output->writeln($message);
    }
}