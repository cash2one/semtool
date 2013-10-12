<?php

define('BASE_PATH', dirname(__FILE__));
$logname = BASE_PATH . '/log/encoding.' . date("Y-m-d") . '.log';

function locallog($s, $flag) {
    global $logname;
    $ns = date('Y-m-d H:i:s') . " [$flag] " . $s . "\n";
    file_put_contents($logname, $ns, FILE_APPEND);
}

if (!isset($_GET['word'])) {
    echo "usage: path?word=xx";
    exit();
}
$word = $_GET['word'];

$flag = '';
if (isset($_GET['flag'])) {
    $flag = $_GET['flag'];
}

$encoding = mb_detect_encoding($word, "UTF-8,GBK,GB2312,CP936,ASCII" );
locallog("$word encoding: $encoding", $flag);

if ($encoding != "UTF-8")
{
        $word = mb_convert_encoding($word, "UTF-8", $encoding);
}

$word_encoded = urlencode($word);
locallog("$word encoding: $encoding, encoded: $word_encoded", $flag);
echo $word_encoded;
