#! /usr/bin/env tarantool

local netbox = require('net.box')
local os = require('os')
local tap = require('tap')
local uri = require('uri')

local test = tap.test('check an extended error')
test:plan(4)


function error_func()
    box.error(box.error.PROC_LUA, "An old good error")
end

function custom_error_func()
    box.error(box.error.CUSTOM_ERROR, "My Custom Type", "A modern custom error")
end

function custom_error_func_2()
    local err = box.error.new(box.error.CUSTOM_ERROR,
                              "My Custom Type", "A modern custom error")
    return err
end

local function version_at_least(peer_version_str, major, minor, patch)
    local major_p, minor_p, patch_p = string.match(peer_version_str,
                                                   "(%d)%.(%d)%.(%d)")
    major_p = tonumber(major_p)
    minor_p = tonumber(minor_p)
    patch_p = tonumber(patch_p)

    if major_p < major
        or (major_p == major and minor_p < minor)
        or (major_p == major and minor_p == minor and patch_p < patch) then
            return false
    end

    return true
end

local function grant_func(user, name)
    box.schema.func.create(name, {if_not_exists = true})
    box.schema.user.grant(user, 'execute', 'function', name, {
        if_not_exists = true
    })
end

local function check_error(err, check_list)
    if type(check_list) ~= 'table' then
        return false
    end

    for k, v in pairs(check_list) do
        if k == 'type' then
            if err.type ~= v then
                return false
            end
        elseif k == 'custom_type' then
            if err.custom_type ~= v then
                return false
            end
        elseif k == 'message' then
            if err.message ~= v then
                return false
            end
        elseif k == 'trace' then
            local trace = "File " .. err.trace[1]['file']
                         .. "\nLine " .. err.trace[1]['line']
            if not string.match(trace, v) then
                return false
            end
        elseif k == 'errno' then
            if err.errno ~= v then
                return false
            end
        elseif k == 'bt' then
            if not string.match(err.bt, v) then
                return false
            end
        elseif k == 'is_custom' then
            if err:is_custom() ~= v then
                return false
            end
        else
            return false
        end
    end

    return true
end

local function test_old_transmission(host, port)
    grant_func('guest', 'error_func')
    grant_func('guest', 'custom_error_func_2')

    local connection = netbox.connect(host, port)
    local _, err = pcall(connection.call, connection, 'error_func')
    local err_2 = connection:call('custom_error_func_2')

    local backtrace_pattern =
    "^stack traceback:\n" ..
    "%s+%[C%]: in function 'new'\n" ..
    "%s+builtin/box/net_box.lua:%d+: in function 'perform_request'\n" ..
    "%s+builtin/box/net_box.lua:%d+: in function '_request'\n" ..
    "%s+builtin/box/net_box.lua:%d+: in function <builtin/box/net_box.lua:%d+>\n" ..
    "%s+%[C%]: in function 'pcall'\n" ..
    ".*box%-tap/extended_error.test.lua:%d+: in function 'test_old_transmission'\n" ..
    ".*box%-tap/extended_error.test.lua:%d+: in main chunk$"

    local check_list = {
        type = 'ClientError',
        message = 'An old good error',
        trace = '^File builtin/box/net_box.lua\nLine %d+$',
        bt = backtrace_pattern,
        is_custom = false
    }

    local check_result = check_error(err, check_list)
    local check_result_2 = type(err_2) == 'string'
        and err_2 == 'User custom error: A modern custom error'

    test:ok(check_result, 'Check the old transmission type(IPROTO_ERROR)')
    test:ok(check_result_2, 'Check the old transmission type(IPROTO_OK)')

    connection:close()
end

local function test_extended_transmission(host, port)
    grant_func('guest', 'custom_error_func')
    grant_func('guest', 'custom_error_func_2')

    local connection = netbox.connect(host, port, {error_extended = true})
    local _, err = pcall(connection.call, connection, 'custom_error_func')
    local err_2 = connection:call('custom_error_func_2')

    local backtrace_pattern =
    "^Remote%(.+:%d+%):\n" ..
    "stack traceback:\n" ..
    "%s+%[C%]: in function 'error'\n" ..
    ".*box%-tap/extended_error.test.lua:%d+: in function <.*box%-tap/extended_error.test.lua:%d+>\n" ..
    "%s+%[C%]: at 0x%w+\n" ..
    "End Remote\n" ..
    "stack traceback:\n" ..
    "%s+builtin/box/net_box.lua:%d+: in function 'parse_extended_error'\n" ..
    "%s+builtin/box/net_box.lua:%d+: in function 'perform_request'\n" ..
    "%s+builtin/box/net_box.lua:%d+: in function '_request'\n" ..
    "%s+builtin/box/net_box.lua:%d+: in function <builtin/box/net_box.lua:%d+>\n" ..
    "%s+%[C%]: in function 'pcall'\n" ..
    ".*box%-tap/extended_error.test.lua:%d+: in function 'test_extended_transmission'\n" ..
    ".*box%-tap/extended_error.test.lua:%d+: in main chunk$"

    local backtrace_pattern_2 =
    "^Remote%(.+:%d+%):\n" ..
    "stack traceback:\n" ..
    "%s+%[C%]: in function 'new'\n" ..
    ".*box%-tap/extended_error.test.lua:%d+: in function <.*box%-tap/extended_error.test.lua:%d+>\n" ..
    "%s+%[C%]: at 0x%w+\n" ..
    "End Remote\n" ..
    "stack traceback:\n" ..
    "%s+builtin/box/net_box.lua:%d+: in function 'parse_extended_error'\n" ..
    "%s+builtin/box/net_box.lua:%d+: in function 'perform_request'\n" ..
    "%s+builtin/box/net_box.lua:%d+: in function '_request'\n" ..
    "%s+builtin/box/net_box.lua:%d+: in function 'call'\n" ..
    ".*box%-tap/extended_error.test.lua:%d+: in function 'test_extended_transmission'\n" ..
    ".*box%-tap/extended_error.test.lua:%d+: in main chunk$"

    local check_list = {
        type = 'CustomError',
        custom_type = 'My Custom Type',
        message = 'User custom error: A modern custom error',
        trace = '^File builtin/box/net_box.lua\nLine %d+$',
        bt = backtrace_pattern,
        is_custom = true
    }

    local check_list_2 = {
        type = 'CustomError',
        custom_type = 'My Custom Type',
        message = 'User custom error: A modern custom error',
        trace = '^File builtin/box/net_box.lua\nLine %d+$',
        bt = backtrace_pattern_2,
        is_custom = true
    }

    local check_result = check_error(err, check_list)
    local check_result_2 = check_error(err_2, check_list_2)
    test:ok(check_result, 'Check the extended transmission type(IPROTO_ERROR)')
    test:ok(check_result_2, 'Check the extended transmission type(IPROTO_OK)')

    connection:close()
end


box.cfg{
    listen = os.getenv('LISTEN')
}
local tarantool_ver = string.match(box.info.version, "%d%.%d%.%d")
local host= uri.parse(box.cfg.listen).host or 'localhost'
local port = uri.parse(box.cfg.listen).service 

if version_at_least(box.info.version, 2, 4, 1) then
    test_extended_transmission(host, port)
else
    test:ok(true, 'Current version of tarantool(' .. tarantool_ver .. ')' ..
            ' don\'t support extended transmission')
    test:ok(true, 'Current version of tarantool(' .. tarantool_ver .. ')' ..
            ' don\'t support extended transmission')
end
test_old_transmission(host, port)

os.exit(test:check() and 0 or 1)
