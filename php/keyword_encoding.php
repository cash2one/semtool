<?php
if (!isset($_GET['word'])) {
    echo "usage: path?word=xx";
    exit();
}

$word = $_GET['word'];
$encodeing = mb_detect_encoding($word, "UTF-8,GBK,GB2312,CP936,ASCII" );
if ($encodeing != "UTF-8")
{
        $word = mb_convert_encoding($word, "UTF-8", $encodeing);
}

$word_encoded = urlencode($word);
echo $word_encoded;
