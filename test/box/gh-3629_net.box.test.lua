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
-- gh-3629: netbox leaks when a connection is closed deliberately
-- and it has non-finished requests.
--
ready = false
ok = nil
err = nil
c = net:connect(box.cfg.listen)
function do_long() while not ready do fiber.sleep(0.01) end end
box.schema.func.create('do_long')
box.schema.user.grant('guest', 'execute', 'function', 'do_long')
f = fiber.create(function() ok, err = pcall(c.call, c, 'do_long') end)
while f:status() ~= 'suspended' do fiber.sleep(0.01) end
c:close()
ready = true
while not err do fiber.sleep(0.01) end
ok, err
