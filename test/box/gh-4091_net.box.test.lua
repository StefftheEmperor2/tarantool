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

space = box.schema.space.create('test', {format={{name="id", type="unsigned"}}})
_ = box.space.test:create_index('primary')
box.schema.user.grant('guest', 'read', 'space', 'test')

c = net.connect(box.cfg.listen)

--
-- gh-4091: index unique flag is always false.
--
c.space.test.index.primary.unique

c:close()
space:drop()
