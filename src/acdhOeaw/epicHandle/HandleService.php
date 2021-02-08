<?php

/*
 * The MIT License
 *
 * Copyright 2017 Austrian Centre for Digital Humanities.
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

namespace acdhOeaw\epicHandle;

use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;
use RuntimeException;

/**
 * Description of HandleService
 *
 * @author zozlak
 */
class HandleService {

    const MOCK_URL = 'http://test';

    private $url;
    private $headers;
    private $client;

    public function __construct($url, $prefix, $login, $pswd) {
        $this->url     = preg_replace('|/?(handles)?/?$|', '', $url) . '/handles/' . $prefix . '/';
        $this->headers = array(
            'Authorization' => 'Basic ' . base64_encode($login . ':' . $pswd),
            'Content-Type'  => 'application/json'
        );
        $this->client  = $this->url !== self::MOCK_URL . '/handles/' . $prefix . '/' ? new Client() : new MockClient;
    }

    public function create($url, $uuid = null, $prefix = null, $suffix = null) {
        $method = $uuid != '' ? 'PUT' : 'POST';
        if ($uuid) {
            $prefix = $prefix ? $prefix . '-' : '';
            $suffix = $suffix ? '-' . $suffix : '';
            $url    = urlencode($prefix . $uuid . $suffix);
        } else {
            $param = array();
            if ($prefix) {
                $param[] = 'prefix=' . urlencode($prefix);
            }
            if ($suffix) {
                $param[] = 'suffix=' . urlencode($suffix);
            }
            $reqUrl = $this->url . '?' . implode('&', $param);
        }

        $request  = new Request($method, $reqUrl, $this->headers, $this->reqData($url));
        $response = $this->client->send($request);
        $pid      = $response->getHeader('Location');
        return $pid[0];
    }

    public function update($pid, $url) {
        $pid      = $this->sanitizePid($pid);
        $request  = new Request('PUT', $pid, $this->headers, $this->reqData($url));
        $response = $this->client->send($request);
        return $response->getStatusCode();
    }

    public function delete($pid) {
        $pid      = $this->sanitizePid($pid);
        $request  = new Request('DELETE', $pid, $this->headers);
        $response = $this->client->send($request);
        return $response->getStatusCode();
    }

    private function reqData($url) {
        return json_encode(array(
            array('type' => 'URL', 'parsed_data' => $url)
        ));
    }

    private function sanitizePid($pid) {
        if (strpos($pid, $this->url) !== 0) {
            if (strpos($pid, '/') !== false) {
                throw new RuntimeException("Wrong PID $pid - expected prefix $this->url");
            }
            $pid = $this->url . $pid;
        }
        return $pid;
    }
}
