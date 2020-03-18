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
-- gh-3400: long-poll input discard must not touch event loop of
-- a closed connection.
--
function long() fiber.yield() return 100 end
c = net.connect(box.cfg.listen)
c:ping()
-- Create batch of two requests. First request is sent to TX
-- thread, second one terminates connection. The preceeding
-- request discards input, and this operation must not trigger
-- new attempts to read any data - the connection is closed
-- already.
--
f = fiber.create(c._transport.perform_request, nil, nil, false, 'call_17', nil, nil, nil, 'long', {}) c._transport.perform_request(nil, nil, false, 'inject', nil, nil, nil, '\x80')
while f:status() ~= 'dead' do fiber.sleep(0.01) end
c:close()
