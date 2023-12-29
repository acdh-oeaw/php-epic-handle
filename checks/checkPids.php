<?php
use GuzzleHttp\Psr7\Request;
include '../vendor/autoload.php';
$prefix = '21.11115';
$limit = $argv[2] ?? 0;
$c = new GuzzleHttp\Client([
    'auth' => ["user$prefix-01", $argv[1]], 
    'http_errors' => false,
    'timeout' => 5,
]);
$service = 'https://pid.gwdg.de/handles';
$r = new Request('get', "$service/$prefix?limit=$limit");
$resp = $c->send($r);
$pids = explode("\n", trim((string) $resp->getBody()));
$pids = array_filter($pids, fn($x) => !str_contains($x, 'USER'));
echo count($pids)." PIDs fetched\n";

if (file_exists('pids.csv')) {
    $checked = explode("\n", trim(file_get_contents('pids.csv')));
    array_shift($checked); // header
    $checked = array_map(fn($x) => explode('/', explode(';', $x)[0])[1], $checked);
    $checked = array_combine($checked, $checked);
    echo "status of " . count($checked) . " PIDs read\n";
    $pids = array_filter($pids, fn($x) => !isset($checked[$x]));
    unset($checked);
    echo count($pids) . " PIDs left to check\n";
    $o = fopen('pids.csv', 'a');
} else {
    $o = fopen('pids.csv', 'w');
    fwrite($o, "pid;status;url\n");
}

$N = count($pids);
$P = max(min($N / 100, 1000), 10);
$n = 1;
$t0 = time();
foreach ($pids as $pid) {
    if ($n % $P === 0) {
        $t = time() - $t0;
        $tl = (int) (($N - $n) / ($t / $n) / 60);
        $t = (int) ($t / 60);
        echo "$n / $N (" . (int) (100 * $n / $N) . "%) $t m elapsed $tl min left\n";
    }
    try {
        $r = $c->send(new Request('get', "$service/$prefix/$pid"));
        $d = json_decode((string) $r->getBody());
        $d = array_filter($d, fn($x) => $x->type === 'URL');
        if (!isset($d[0])) {
            fwrite($o, "$prefix/$pid;no URL;" . json_encode((string) $r->getBody(),  JSON_UNESCAPED_SLASHES) . "\n");
        } elseif(empty(trim($d[0]->parsed_data))) {
            fwrite($o, "$prefix/$pid;empty URL;" . json_encode((string) $r->getBody(),  JSON_UNESCAPED_SLASHES) . "\n");
        } else {
            $d = $d[0]->parsed_data;
            $r = $c->send(new Request('head', $d));
            fwrite($o, "$prefix/$pid;" . $r->getStatusCode() . ";$d\n");
        }
    } catch (Throwable $e) {
        fwrite($o, "$prefix/$pid;HTTP error;" . json_encode(['message' => $e->getMessage(), 'piddata' => (string) $r->getBody()], JSON_UNESCAPED_SLASHES) . "\n"); 
    }
    $n++;
}
fclose($o);
