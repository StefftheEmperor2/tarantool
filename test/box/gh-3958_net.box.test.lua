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
-- gh-3958 updating box.cfg.readahead doesn't affect existing connections.
--
readahead = box.cfg.readahead

box.cfg{readahead = 128}

s = box.schema.space.create("test")
_ = s:create_index("pk")
box.schema.user.grant("guest", "read,write", "space", "test")

-- connection is created with small readahead value,
-- make sure it is updated if box.cfg.readahead is changed.
c = net.connect(box.cfg.listen)

box.cfg{readahead = 100 * 1024}

box.error.injection.set("ERRINJ_WAL_DELAY", true)
pad = string.rep('x', 8192)
for i = 1, 5 do c.space.test:replace({i, pad}, {is_async = true}) end
box.error.injection.set("ERRINJ_WAL_DELAY", false)

test_run:wait_log('default', 'readahead limit is reached', 1024, 0.1)

s:drop()
box.cfg{readahead = readahead}
