%%%-------------------------------------------------------------------
%%% @author losto
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. Nov 2020 9:50 PM
%%%-------------------------------------------------------------------
-module(basic_SUITE).
-author("losto").
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([]).

all() ->
    [
        check_start,
        check_environment,
        check_table_create_bad_type,
        check_table_create_success,
        check_table_create_double,
        check_table_delete,
        check_table_delete_unknown,
        check_put_bad_tuple,
        check_put_bad_table,
        check_put_success
    ].

init_per_suite(InitConfig) ->
    os:cmd("rm -rf /tmp/edb"),
    os:cmd("mkdir -p /tmp/edb/journal"),
    application:ensure_all_started(lsm_engine),
    application:set_env(lsm_engine, max_ram_table, 128),
    application:set_env(lsm_engine, test, true),
    InitConfig.

init_per_testcase(check_put_bad_tuple, Config) ->
    lsm_engine:table_create("t1", disk),
    Config;
init_per_testcase(_, Config) ->
    Config.

end_per_suite(Config) ->
    os:cmd("rm -rf /tmp/edb"),
    Config.

check_start(_Config) ->
    Result = application:get_application(lsm_engine),
    ?assertEqual({ok,lsm_engine}, Result).

check_environment(_Config) ->
    MaxRam = application:get_env(lsm_engine, max_ram_table, 1000000),
    Test = application:get_env(lsm_engine, test, false),
    ?assertEqual({128, true}, {MaxRam, Test}).

check_table_create_bad_type(_Config) ->
    Result = lsm_engine:table_create("t1", qqq),
    ?assertEqual({error, {bad_table_type, qqq}}, Result).

check_table_create_success(_) ->
    Result = lsm_engine:table_create("t1", disk),
    ?assertEqual(ok, Result).

check_table_create_double(_) ->
    Result = lsm_engine:table_create("t1", disk),
    ?assertEqual({error, eexist}, Result).

check_table_delete(_) ->
    Result = lsm_engine:table_delete("t1"),
    ?assertEqual(ok, Result).

check_table_delete_unknown(_) ->
    Result = lsm_engine:table_delete("t1"),
    ?assertEqual({error,enoent}, Result).

check_put_bad_tuple(_) ->
    Result = lsm_engine:put("t1", {1,2,3}),
    ?assertEqual({error, {bad_records, [{1,2,3}]}}, Result).

check_put_bad_table(_) ->
    Result = lsm_engine:put("t2", {1,2}),
    ?assertEqual({error,{'EXIT',{noproc,{gen_server,call,[t2,{put,[{1,2}]}]}}}}, Result).

check_put_success(_) ->
    Record = {erlang:system_time(), crypto:strong_rand_bytes(16)},
    Result = {ok, OpId} = lsm_engine:put("t1", Record),
    ?assertEqual({ok, OpId}, Result),
    {CPath, FPath} = lsm_engine_journal:get_journals(),
    {ok, CPath} = disk_log:open([{name, CPath}, {file, CPath}]),
    {ok, FPath} = disk_log:open([{name, FPath}, {file, FPath}]),
    {_, CTerms} = disk_log:chunk(CPath, start, infinity),
    ?assertEqual([{OpId, {put, [Record]}}], CTerms),
    ?assertEqual(eof, disk_log:chunk(FPath, start, infinity)).

check_put_with_rotation(_) -> ok.
check_put_with_dumping(_) -> ok.
check_put_with_rotation_after_dumping(_) -> ok.