%%%-------------------------------------------------------------------
%%% @author losto
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. Nov 2020 3:33 PM
%%%-------------------------------------------------------------------
-module(test).
-author("losto").
-compile([export_all]).
%% API
-export([start/2,
    test/2,
    insert/3]).

start(Tab, MaxCount) ->
    inserting(Tab, MaxCount, 0).

inserting(_Tab, MaxCount, MaxCount) -> MaxCount;
inserting(Tab, MaxCount, CurCount) ->
    %{Time, _} = timer:tc(lsm_engine, put, [Tab, {erlang:system_time(), crypto:strong_rand_bytes(64)}]),
    lsm_engine:put(Tab, {erlang:system_time(), crypto:strong_rand_bytes(64)}),
    inserting(Tab, MaxCount, CurCount + 1).

test(TabCount, RecCount) ->
    ListTables = create_tables(TabCount, []),
    lists:foreach(fun(Table) -> spawn(test, insert, [Table, RecCount, 0]) end, ListTables).


insert(_Table, RecCount, RecCount) -> finish;
insert(Table, RecCount, Count) ->
    lsm_engine:put(Table, {erlang:system_time(), crypto:strong_rand_bytes(64)}),
    timer:sleep(150),
    insert(Table, RecCount, Count + 1).

create_tables(N) ->
    create_tables(N, []).
create_tables(0, ListTables) -> ListTables;
create_tables(TabCount, ListTables) ->
    Table = erlang:integer_to_list(TabCount),
    lsm_engine:table_create(Table, disk),
    create_tables(TabCount - 1, [Table | ListTables]).

crt(N) ->
    lists:foreach(fun(_X) -> spawn(?MODULE, crypto_test, [1000]) end, lists:seq(1, N)).

crypto_test(0) -> finish;
crypto_test(N) ->
    {erlang:system_time(), crypto:strong_rand_bytes(64)},
    timer:sleep(150),
    crypto_test(N - 1).