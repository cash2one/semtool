<?php

define('BASE_PATH', dirname(__FILE__));
$logname = BASE_PATH . '/log/encoding.' . date("Y-m-d") . '.log';

function locallog($s) {
    global $logname;
    $ns = date('H-i-s: ') . $s . "\n";
    file_put_contents($logname, $ns, FILE_APPEND);
}


$_GET['word'] = '中国';

if (!isset($_GET['word'])) {
    echo "usage: path?word=xx";
    exit();
}

$word = $_GET['word'];
$encoding = mb_detect_encoding($word, "UTF-8,GBK,GB2312,CP936,ASCII" );
locallog("$word encoding: $encoding");

if ($encoding != "UTF-8")
{
        $word = mb_convert_encoding($word, "UTF-8", $encoding);
}

$word_encoded = urlencode($word);
locallog("$word encoding: $encoding, encoded: $word_encoded");
echo $word_encoded;
