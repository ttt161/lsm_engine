%%%-------------------------------------------------------------------
%%% @author losto
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(lsm_engine_mgr).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([
         register/1,
         unregister/1,
         is_exists/1,
         list/0
        ]).

-include("lsm_engine.hrl").
-define(SERVER, ?MODULE).

-record(lsm_engine_mgr_state, {tables, file}).

%%%===================================================================
%%% API
%%%===================================================================

register(TableName) ->
    gen_server:call(?MODULE, {register, TableName}).

unregister(TableName) ->
    gen_server:call(?MODULE, {unregister, TableName}).

is_exists(TableName) ->
    gen_server:call(?MODULE, {is_exists, TableName}).

list() ->
    gen_server:call(?MODULE, list).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    DbDir = lsm_engine_common:get_db_directory(),
    File = filename:join([DbDir, ?SCHEMA_FILE]),
    {ok, Name} = dets:open_file(?SCHEMA_FILE, [{file, File}]),
    SetOfTables = case dets:lookup(Name, tables) of
                      [] ->
                          EmptyTables = sets:new(),
                          dets:insert(Name, EmptyTables),
                          EmptyTables;
                      [{tables, Set}] -> Set
                  end,
    start_engines(SetOfTables),
    {ok, #lsm_engine_mgr_state{tables = SetOfTables, file = Name}}.

handle_call({register, TableName}, _From, State = #lsm_engine_mgr_state{
                                                     tables = SetsOfTables, file = File
                                                    }) ->

    {Result, NewSet} = case sets:is_element(TableName, SetsOfTables) of
                           true -> {{error, exists}, SetsOfTables};
                           false ->
                               New = sets:add_element(TableName, SetsOfTables),
                               case dets:insert(File, {tables, New}) of
                                   ok ->
                                       supervisor:start_child(lsm_engine_sup, #{
                                                                                id => TableName,
                                                                                start => {lsm_engine, start_link, [TableName]}
                                                                               }),
                                       {ok, New};
                                   Error -> {Error, SetsOfTables}
                               end
                       end,
    {reply, Result, State#lsm_engine_mgr_state{tables = NewSet}};
handle_call({unregister, TableName}, _From, State = #lsm_engine_mgr_state{
                                                       tables = SetsOfTables, file = File
                                                      }) ->

    NewSet = sets:del_element(TableName, SetsOfTables),
    case dets:insert(File, {tables, NewSet}) of
        ok ->
            supervisor:terminate_child(lsm_engine_sup, TableName),
            {reply, ok, State#lsm_engine_mgr_state{tables = NewSet}};
        Error ->
            {reply, Error, State}
    end;
handle_call({is_exists, TableName}, _From, State = #lsm_engine_mgr_state{
                                                      tables = SetsOfTables}) ->

    Result = sets:is_element(TableName, SetsOfTables),
    {reply, Result, State};
handle_call(list, _From, State = #lsm_engine_mgr_state{
                                    tables = SetsOfTables}) ->

    Result = sets:to_list(SetsOfTables),
    {reply, Result, State};
handle_call(_Request, _From, State = #lsm_engine_mgr_state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #lsm_engine_mgr_state{}) ->
    {noreply, State}.

handle_info(_Info, State = #lsm_engine_mgr_state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #lsm_engine_mgr_state{}) ->
    ok.

code_change(_OldVsn, State = #lsm_engine_mgr_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_engines(SetOfTables) ->
    sets:fold(fun(TableName, _Acc) ->
                      ChildSpec = #{
                                    id => TableName,
                                    start => {lsm_engine, start_link, [TableName]}
                                   },
                      supervisor:start_child(lsm_engine_sup, ChildSpec)
              end, [], SetOfTables).
