#!/usr/bin/php
<?php

/*
 * The MIT License
 *
 * Copyright 2023 zozlak.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

$composerDir = realpath(__DIR__);
while ($composerDir !== false && !file_exists("$composerDir/vendor")) {
    $composerDir = realpath("$composerDir/..");
}
require_once "$composerDir/vendor/autoload.php";

use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;

$parser = new zozlak\argparse\ArgumentParser();
$parser->addArgument('--service', default: 'https://pid.gwdg.de/handles');
$parser->addArgument('--prefix', default: '21.11115');
$parser->addArgument('--login', help: 'hdl service login (by default "user{prefix}-01"');
$parser->addArgument('--limit', default: 0, help: 'maximum number of handles to fetch (by default all)');
$parser->addArgument('--repoLogin');
$parser->addArgument('--repoPswd');
$parser->addArgument('--repoNmsp', default: 'https://arche.acdh.oeaw.ac.at/api/');
$parser->addArgument('--output', default: 'pids.csv');
$parser->addArgument('--nParallel', type: 'int', default: 5);
$parser->addArgument('--timeout', type: 'int', default: 10);
$parser->addArgument('--chunk', type: 'int', default: 1000);
$parser->addArgument('hdlPassword');
$args = $parser->parseArgs();

$login = $args->login ?? "user$args->prefix-01";

$limit = 0;

$cHdl = new GuzzleHttp\Client(['auth' => [$login, $args->hdlPassword]]);
$r = new Request('get', "$args->service/$args->prefix?limit=$limit");
$resp = $cHdl->send($r);
$pids = explode("\n", trim((string) $resp->getBody()));
$pids = array_filter($pids, fn($x) => !str_contains($x, 'USER') && !empty($x));
$pids = array_map(fn($x) => "$args->prefix/$x", $pids);
echo count($pids)." PIDs fetched\n";

if (file_exists($args->output)) {
    $checked = explode("\n", trim(file_get_contents($args->output)));
    array_shift($checked); // header
    $checked = array_map(fn($x) => explode(';', $x)[0], $checked);
    $checked = array_combine($checked, $checked);
    echo count($checked) . " PIDs already checked\n";
    $pids = array_filter($pids, fn($x) => !isset($checked[$x]));
    unset($checked);
    echo count($pids) . " PIDs left to check\n";
    $o = fopen($args->output, 'a');
} else {
    $o = fopen($args->output, 'w');
    fwrite($o, "pid;status;url;finalurl;data\n");
}
$pids = array_values($pids);

$cRes = new GuzzleHttp\Client([
    'http_errors' => false,
    'timeout' => $args->timeout,
    'allow_redirects' => [
        'max' => 5,
        'track_redirects' => true,
    ]
]);
$cAuth = null;
if (!empty($args->repoLogin) && !empty($args->repoPswd) && !empty($args->repoNmsp)) {
    $cAuth = new GuzzleHttp\Client([
        'auth' => [$args->repoLogin, $args->repoPswd], 
        'http_errors' => false,
        'timeout' => $args->timeout,
        'allow_redirects' => [
            'max' => 5,
            'track_redirects' => true,
        ]
    ]);
}

$T0 = time();
$N = count($pids);
$n = 0;
while (count($pids) > 0) {
    $pidsTmp = array_splice($pids, max(0, count($pids) - $args->chunk), $args->chunk);
    $reqTmp = array_map(fn($x) => new Request('get', "$args->service/$x"), $pidsTmp);
    $urlsTmp = [];
    $pool = new GuzzleHttp\Pool(
        $cHdl, 
        $reqTmp, 
        [
            'concurrency' => $args->nParallel,
            'fulfilled' => function(Response $r, int $i) use ($args, $pidsTmp, $o, &$urlsTmp) {
                $pid = $pidsTmp[$i];
                $d = json_decode((string) $r->getBody());
                $d = array_filter($d, fn($x) => $x->type === 'URL');
                if (!isset($d[0])) {
                    fwrite($o, "$pid;no URL;;;" . json_encode((string) $r->getBody(),  JSON_UNESCAPED_SLASHES) . "\n");
                } elseif(empty(trim($d[0]->parsed_data))) {
                    fwrite($o, "$pid;empty URL;;;" . json_encode((string) $r->getBody(),  JSON_UNESCAPED_SLASHES) . "\n");
                } else {
                    $urlsTmp[$i] = $d[0]->parsed_data;
                }
            },
            'rejected' => function(GuzzleHttp\Exception\RequestException $e, int $i) use ($pidsTmp, $o){
                fwrite($o, $pidsTmp[$i]. ";failed to read PID data;;;" . json_encode($e) . "\n");
            }
        ]
    );
    $pool->promise()->wait();

    $reqTmp = array_map(fn($x) => new Request('get', $x), $urlsTmp);
    $pool = new GuzzleHttp\Pool(
        $cRes, 
        $reqTmp, 
        [
            'concurrency' => $args->nParallel,
            'fulfilled' => function(Response $r, int $i) use ($args, $pidsTmp, $o, $urlsTmp, $cAuth) {
                $pid = $pidsTmp[$i];
                $url = $urlsTmp[$i];
                $finalUrl = $r->getHeader('X-Guzzle-Redirect-History');
                $finalUrl = end($finalUrl) ?? $url;
                if ($cAuth !== null && ($r->getStatusCode() === 403 || $r->getStatusCode() === 401) && str_starts_with($finalUrl, $args->repoNmsp)) {
                    $r = $cAuth->send(new Request('head', $finalUrl));
                    $finalUrl = $r->getHeader('X-Guzzle-Redirect-History');
                    $finalUrl = end($finalUrl);
                }
                fwrite($o, "$pid;" . $r->getStatusCode() . ";$url;$finalUrl;\n");
            },
            'rejected' => function(Psr\Http\Client\NetworkExceptionInterface $e, int $i) use ($pidsTmp, $urlsTmp, $o){
                $pid = $pidsTmp[$i];
                $url = $urlsTmp[$i];
                $finalUrl = $e->getRequest()->getUri();
                $msg = $e->getMessage();
                if (str_starts_with($msg, 'cURL error 6:')) {
                    $status = 'Could not resolve host';
                } elseif (str_starts_with($msg, 'cURL error 28:')) {
                    $status = 'Connection timeout';
                }
                if (isset($status)) {
                    fwrite($o, "$pid;$status;$url;$finalUrl;\n");
                } else {
                    print_r([$pid, $msg, $url, $finalUrl]);
                }
            }
        ]
    );
    $pool->promise()->wait();

    $n += count($pidsTmp);
    $t = (time() - $T0) / 60;
    echo "$n / $N (" . round(100 * $n / $N, 1) . "%) " . round($t) . "m elapsed, ETA " . round(($N - $n) * ($t / $n)) . "m\n";
}
fclose($o);
