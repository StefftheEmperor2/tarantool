test_run = require('test_run').new()
net = require('net.box')

-- gh-970 gh-971 UPSERT over network
_ = box.schema.space.create('test')
_ = box.space.test:create_index('primary', {type = 'TREE', parts = {1,'unsigned'}})
_ = box.space.test:create_index('covering', {type = 'TREE', parts = {1,'unsigned',3,'string',2,'unsigned'}})
_ = box.space.test:insert{1, 2, "string"}
box.schema.user.grant('guest', 'read,write', 'space', 'test')
c = net:connect(box.cfg.listen)
c.space.test:select{}
c.space.test:upsert({1, 2, 'nothing'}, {{'+', 2, 1}}) -- common update
c.space.test:select{}
c.space.test:upsert({2, 4, 'something'}, {{'+', 2, 1}}) -- insert
c.space.test:select{}
c.space.test:upsert({2, 4, 'nothing'}, {{'+', 3, 100500}}) -- wrong operation
c.space.test:select{}
