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

--
-- Check that it's possible to get connection object form net.box space
--

space = box.schema.space.create('test', {format={{name="id", type="unsigned"}}})
space ~= nil
_ = box.space.test:create_index('primary')
box.schema.user.grant('guest','read,write,execute','space', 'test')

c = net.connect(box.cfg.listen)

c:ping()
c.space.test ~= nil

c.space.test.connection == c
box.schema.user.revoke('guest','read,write,execute','space', 'test')
c:close()
