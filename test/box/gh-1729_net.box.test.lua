remote = require 'net.box'
fiber = require 'fiber'
log = require 'log'
msgpack = require 'msgpack'
env = require('test_run')
test_run = env.new()
test_run:cmd("push filter ".."'\\.lua.*:[0-9]+: ' to '.lua...\"]:<line>: '")

net = require('net.box')

_ = box.schema.space.create('test')
_ = box.space.test:create_index('primary', {type = 'TREE', parts = {1,'unsigned'}})
_ = box.space.test:create_index('covering', {type = 'TREE', parts = {1,'unsigned',3,'string',2,'unsigned'}})
_ = box.space.test:insert{1, 2, "string"}
box.schema.user.grant('guest', 'read,write', 'space', 'test')
c = net:connect(box.cfg.listen)

-- gh-1729 net.box index metadata incompatible with local metadata
c.space.test.index.primary.parts
c.space.test.index.covering.parts

box.space.test:drop()

-- CALL vs CALL_16 in connect options
function echo(...) return ... end
box.schema.user.grant('guest', 'execute', 'universe')
c = net.connect(box.cfg.listen)
c:call('echo', {42})
c:eval('return echo(...)', {42})
-- invalid arguments
c:call('echo', 42)
c:eval('return echo(...)', 42)
c:close()
c = net.connect(box.cfg.listen, {call_16 = true})
c:call('echo', 42)
c:eval('return echo(...)', 42)
c:close()
box.schema.user.revoke('guest', 'execute', 'universe')
