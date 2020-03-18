remote = require 'net.box'
test_run = require('test_run').new()
LISTEN = require('uri').parse(box.cfg.listen)

-- #1545 empty password
cn = remote.connect(LISTEN.host, LISTEN.service, { user = 'test' })
cn ~= nil
cn:close()
cn = remote.connect(LISTEN.host, LISTEN.service, { password = 'test' })
cn ~= nil
cn:close()
