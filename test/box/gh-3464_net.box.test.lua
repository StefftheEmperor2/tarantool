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
-- gh-3464: iproto hangs in 100% CPU when too big packet size
-- is received due to size_t overflow.
--
c = net:connect(box.cfg.listen)
data = msgpack.encode(18400000000000000000)..'aaaaaaa'
c._transport.perform_request(nil, nil, false, 'inject', nil, nil, nil, data)
c:close()
test_run:grep_log('default', 'too big packet size in the header') ~= nil
