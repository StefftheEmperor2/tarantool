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
-- gh-3107: fiber-async netbox.
--
cond = nil
box.schema.func.create('long_function')
box.schema.user.grant('guest', 'execute', 'function', 'long_function')
function long_function(...) cond = fiber.cond() cond:wait() return ... end
function finalize_long() while not cond do fiber.sleep(0.01) end cond:signal() cond = nil end
s = box.schema.create_space('test')
pk = s:create_index('pk')
s:replace{1}
s:replace{2}
s:replace{3}
s:replace{4}
c = net:connect(box.cfg.listen)
--
-- Check long connections, multiple wait_result().
--
future = c:call('long_function', {1, 2, 3}, {is_async = true})
future:result()
future:is_ready()
future:wait_result(0.01) -- Must fail on timeout.
finalize_long()
ret = future:wait_result(100)
future:is_ready()
-- Any timeout is ok - response is received already.
future:wait_result(0)
future:wait_result(0.01)
ret

_, err = pcall(future.wait_result, future, true)
err:find('Usage') ~= nil
_, err = pcall(future.wait_result, future, '100')
err:find('Usage') ~= nil

--
-- Check infinity timeout.
--
ret = nil
_ = fiber.create(function() ret = c:call('long_function', {1, 2, 3}, {is_async = true}):wait_result() end)
finalize_long()
while not ret do fiber.sleep(0.01) end
ret
c:close()
box.schema.user.grant('guest', 'execute', 'universe')
c = net:connect(box.cfg.listen)
future = c:eval('return long_function(...)', {1, 2, 3}, {is_async = true})
future:result()
future:wait_result(0.01) -- Must fail on timeout.
finalize_long()
future:wait_result(100)

c:close()
--
-- Check that is_async does not work on a closed connection.
--
c:call('any_func', {}, {is_async = true})

box.schema.user.revoke('guest', 'execute', 'universe')
c = net:connect(box.cfg.listen)

--
-- Ensure the request is garbage collected both if is not used and
-- if is.
--
gc_test = setmetatable({}, {__mode = 'v'})
gc_test.future = c:call('long_function', {1, 2, 3}, {is_async = true})
gc_test.future ~= nil
collectgarbage()
gc_test
finalize_long()

future = c:call('long_function', {1, 2, 3}, {is_async = true})
collectgarbage()
future ~= nil
finalize_long()
future:wait_result(1000)
collectgarbage()
future ~= nil
gc_test.future = future
future = nil
collectgarbage()
gc_test

--
-- Ensure a request can be finalized from non-caller fibers.
--
future = c:call('long_function', {1, 2, 3}, {is_async = true})
ret = {}
count = 0
for i = 1, 10 do fiber.create(function() ret[i] = future:wait_result(1000) count = count + 1 end) end
future:wait_result(0.01) -- Must fail on timeout.
finalize_long()
while count ~= 10 do fiber.sleep(0.1) end
ret

--
-- Test space methods.
--
c:close()
box.schema.user.grant('guest', 'read,write', 'space', 'test')
c = net:connect(box.cfg.listen)
future = c.space.test:select({1}, {is_async = true})
ret = future:wait_result(100)
ret
type(ret[1])
future = c.space.test:insert({5}, {is_async = true})
future:wait_result(100)
s:get{5}
future = c.space.test:replace({6}, {is_async = true})
future:wait_result(100)
s:get{6}
future = c.space.test:delete({6}, {is_async = true})
future:wait_result(100)
s:get{6}
future = c.space.test:update({5}, {{'=', 2, 5}}, {is_async = true})
future:wait_result(100)
s:get{5}
future = c.space.test:upsert({5}, {{'=', 2, 6}}, {is_async = true})
future:wait_result(100)
s:get{5}
future = c.space.test:get({5}, {is_async = true})
future:wait_result(100)

--
-- Test index methods.
--
future = c.space.test.index.pk:select({1}, {is_async = true})
future:wait_result(100)
future = c.space.test.index.pk:get({2}, {is_async = true})
future:wait_result(100)
future = c.space.test.index.pk:min({}, {is_async = true})
future:wait_result(100)
future = c.space.test.index.pk:max({}, {is_async = true})
future:wait_result(100)
c:close()
box.schema.user.grant('guest', 'execute', 'universe')
c = net:connect(box.cfg.listen)
future = c.space.test.index.pk:count({3}, {is_async = true})
future:wait_result(100)
c:close()
box.schema.user.revoke('guest', 'execute', 'universe')
c = net:connect(box.cfg.listen)
future = c.space.test.index.pk:delete({3}, {is_async = true})
future:wait_result(100)
s:get{3}
future = c.space.test.index.pk:update({4}, {{'=', 2, 6}}, {is_async = true})
future:wait_result(100)
s:get{4}

--
-- Test async errors.
--
future = c.space.test:insert({1}, {is_async = true})
future:wait_result()
future:result()

--
-- Test discard.
--
future = c:call('long_function', {1, 2, 3}, {is_async = true})
future:discard()
finalize_long()
future:result()
future:wait_result(100)

--
-- Test closed connection.
--
future = c:call('long_function', {1, 2, 3}, {is_async = true})
finalize_long()
future:wait_result(100)
future2 = c:call('long_function', {1, 2, 3}, {is_async = true})
c:close()
future2:wait_result(100)
future2:result()
future2:discard()
-- Already successful result must be available.
future:wait_result(100)
future:result()
future:is_ready()
finalize_long()

--
-- Test reconnect.
--
c = net:connect(box.cfg.listen, {reconnect_after = 0.01})
future = c:call('long_function', {1, 2, 3}, {is_async = true})
_ = c._transport.perform_request(nil, nil, false, 'inject', nil, nil, nil, '\x80')
while not c:is_connected() do fiber.sleep(0.01) end
finalize_long()
future:wait_result(100)
future:result()
future = c:call('long_function', {1, 2, 3}, {is_async = true})
finalize_long()
future:wait_result(100)

--
-- Test raw response getting.
--
ibuf = require('buffer').ibuf()
future = c:call('long_function', {1, 2, 3}, {is_async = true, buffer = ibuf})
finalize_long()
future:wait_result(100)
result, ibuf.rpos = msgpack.decode_unchecked(ibuf.rpos)
result

box.schema.func.drop('long_function')

--
-- Test async schema version change.
--
function change_schema(i) local tmp = box.schema.create_space('test'..i) return 'ok' end
box.schema.func.create('change_schema')
box.schema.user.grant('guest', 'execute', 'function', 'change_schema')
box.schema.user.grant('guest', 'write', 'space', '_schema')
box.schema.user.grant('guest', 'read,write', 'space', '_space')
box.schema.user.grant('guest', 'create', 'space')
future1 = c:call('change_schema', {'1'}, {is_async = true})
future2 = c:call('change_schema', {'2'}, {is_async = true})
future3 = c:call('change_schema', {'3'}, {is_async = true})
future1:wait_result()
future2:wait_result()
future3:wait_result()

c:close()
s:drop()
box.space.test1:drop()
box.space.test2:drop()
box.space.test3:drop()
box.schema.func.drop('change_schema')
box.schema.user.revoke('guest', 'write', 'space', '_schema')
box.schema.user.revoke('guest', 'read,write', 'space', '_space')
box.schema.user.revoke('guest', 'create', 'space')
