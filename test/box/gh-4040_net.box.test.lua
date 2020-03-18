remote = require 'net.box'
fiber = require 'fiber'
log = require 'log'
msgpack = require 'msgpack'
env = require('test_run')
test_run = env.new()
test_run:cmd("push filter ".."'\\.lua.*:[0-9]+: ' to '.lua...\"]:<line>: '")

LISTEN = require('uri').parse(box.cfg.listen)
space = box.schema.space.create('net_box_test_space')
index = space:create_index('primary', { type = 'tree' })

box.schema.user.create('netbox', { password  = 'test' })
box.schema.user.grant('netbox', 'read,write', 'space', 'net_box_test_space')
box.schema.user.grant('netbox', 'execute', 'universe')

net = require('net.box')
socket = require('socket');

test_run:cmd('create server connecter with script = "box/proxy.lua"')

--
-- related to gh-4040: log corrupted rows
--
log_level = box.cfg.log_level
box.cfg{log_level=6}
sock = socket.tcp_connect(LISTEN.host, LISTEN.service)
sock:read(9)
-- we need to have a packet with correctly encoded length,
-- so that it bypasses iproto length check, but cannot be
-- decoded in xrow_header_decode
-- 0x3C = 60, sha1 digest is 20 bytes long
data = string.fromhex('3C'..string.rep(require('digest').sha1_hex('bcde'), 3))
sock:write(data)
sock:close()

test_run:wait_log('default', 'Got a corrupted row.*', nil, 10)
test_run:wait_log('default', '00000000:.*', nil, 10)
test_run:wait_log('default', '00000010:.*', nil, 10)
test_run:wait_log('default', '00000020:.*', nil, 10)
test_run:wait_log('default', '00000030:.*', nil, 10)
-- we expect nothing below, so don't wait
test_run:grep_log('default', '00000040:.*')

box.cfg{log_level=log_level}
