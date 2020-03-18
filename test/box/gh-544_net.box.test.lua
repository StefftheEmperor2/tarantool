remote = require 'net.box'
test_run = require('test_run').new()

LISTEN = require('uri').parse(box.cfg.listen)
box.schema.user.create('netbox', { password  = 'test' })

-- #544 usage for remote[point]method
cn = remote.connect(LISTEN.host, LISTEN.service)

box.schema.user.grant('guest', 'execute', 'universe')
cn:close()
cn = remote.connect(LISTEN.host, LISTEN.service)
cn:eval('return true')
cn.eval('return true')

cn.ping()

cn:close()

remote.self:eval('return true')
remote.self.eval('return true')
box.schema.user.revoke('guest', 'execute', 'universe')

-- uri as the first argument
uri = string.format('%s:%s@%s:%s', 'netbox', 'test', LISTEN.host, LISTEN.service)

cn = remote.new(uri)
cn:ping()
cn:close()

uri = string.format('%s@%s:%s', 'netbox', LISTEN.host, LISTEN.service)
cn = remote.new(uri)
cn ~= nil, cn.state, cn.error
cn:close()
-- don't merge creds from uri & opts
remote.new(uri, { password = 'test' })
cn = remote.new(uri, { user = 'netbox', password = 'test' })
cn:ping()
cn:close()

box.schema.user.drop('netbox')
