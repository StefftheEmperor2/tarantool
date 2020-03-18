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
-- gh-2978: field names for tuples received from netbox.
--
_ = box.schema.create_space("named", {format = {{name = "id"}, {name="abc"}}})
_ = box.space.named:create_index('id', {parts = {{1, 'unsigned'}}})
box.space.named:insert({1, 1})
box.schema.user.grant('guest', 'read, write, execute', 'space')
cn = net.connect(box.cfg.listen)

s = cn.space.named
s:get{1}.id
s:get{1}:tomap()
s:insert{2,3}:tomap()
s:replace{2,14}:tomap()
s:update(1, {{'+', 2, 10}}):tomap()
s:select()[1]:tomap()
s:delete({2}):tomap()

-- Check that formats changes after reload.
box.space.named:format({{name = "id2"}, {name="abc2"}})
s:select()[1]:tomap()
cn:reload_schema()
s:select()[1]:tomap()

cn:close()
box.space.named:drop()
box.schema.user.revoke('guest', 'read, write, execute', 'space')
