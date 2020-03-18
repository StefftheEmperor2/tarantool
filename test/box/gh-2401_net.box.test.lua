remote = require 'net.box'
test_run = require('test_run').new()

space = box.schema.space.create('net_box_test_space')
index = space:create_index('primary', { type = 'tree' })

box.schema.user.create('netbox', { password  = 'test' })
box.schema.user.grant('netbox', 'read,write', 'space', 'net_box_test_space')
box.schema.user.grant('netbox', 'execute', 'universe')

net = require('net.box')
socket = require('socket');

test_run:cmd('create server connecter with script = "box/proxy.lua"')

--
-- gh-2401 update pseudo objects not replace them
--
space:drop()
space = box.schema.space.create('test')
box.schema.user.grant('guest', 'read', 'space', 'test')
c = net.connect(box.cfg.listen)
cspace = c.space.test
space.index.test_index == nil
cspace.index.test_index == nil
_ = space:create_index("test_index", {parts={1, 'string'}})
c:reload_schema()
space.index.test_index ~= nil
cspace.index.test_index ~= nil
c.space.test.index.test_index ~= nil

-- cleanup

space:drop()
