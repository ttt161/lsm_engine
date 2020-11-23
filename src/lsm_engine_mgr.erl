%%%-------------------------------------------------------------------
%%% @author losto
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(lsm_engine_mgr).

-behaviour(gen_server).

-export([start_link/1]).
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
    gen_server:call(?MODULE, list, infinity).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(DbDir) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [DbDir], []).

init([DbDir]) ->
    File = filename:join([DbDir, ?SCHEMA_FILE]),
    {ok, Name} = dets:open_file(?SCHEMA_FILE, [{file, File}]),
    SetOfTables = case dets:lookup(Name, tables) of
                      [] ->
                          EmptyTables = sets:new(),
                          dets:insert(Name, EmptyTables),
                          EmptyTables;
                      [{tables, Set}] -> Set
                  end,
    self() ! start_engines,
    {ok, #lsm_engine_mgr_state{tables = SetOfTables, file = Name}}.

handle_call({register, TableName}, _From, State) ->
    {Result, NewState} =  case catch do_register(TableName, State) of
                              {ok, NewSt} -> {ok, NewSt};
                              {{error, Reason}, NewSt} -> {{error, Reason}, NewSt};
                              Other -> {{error, Other}, State}
                          end,
    {reply, Result, NewState};
handle_call({unregister, TableName}, _From, State = #lsm_engine_mgr_state{
                                                       tables = SetsOfTables, file = File
                                                      }) ->

    NewSet = sets:del_element(TableName, SetsOfTables),
    case catch dets:insert(File, {tables, NewSet}) of
        ok ->
            supervisor:terminate_child(lsm_engine_sup, TableName),
            supervisor:delete_child(lsm_engine_sup, TableName),
            {reply, ok, State#lsm_engine_mgr_state{tables = NewSet}};
        {error, Reason} ->
            {reply, {error, Reason}, State};
        Other -> {reply, {error, Other}, State}
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

handle_info(start_engines, State = #lsm_engine_mgr_state{tables = SetOfTables}) ->
    sets:fold(fun(TableName, _Acc) ->
                      ChildSpec = #{
                                    id => TableName,
                                    start => {lsm_engine, start_link, [TableName]}
                                   },
                      case supervisor:start_child(lsm_engine_sup, ChildSpec) of
                          {ok, _} -> ok;
                          {ok, _, _} -> ok;
                          _ -> throw({error, cant_start_engine})
                      end
              end, [], SetOfTables),
    %timer:apply_after(?ROTATION_PERIOD, lsm_engine_journal, rotation, [?TEST]),
    {noreply, State};
handle_info(_Info, State = #lsm_engine_mgr_state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #lsm_engine_mgr_state{}) ->
    ok.

code_change(_OldVsn, State = #lsm_engine_mgr_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_register(TableName, State) ->
    do_register(is_exists, TableName, State).

do_register(is_exists, TableName, State = #lsm_engine_mgr_state{tables = SetOfTables}) ->
    case sets:is_element(TableName, SetOfTables) of
        true -> {{error, eexist}, State};
        false -> do_register(update_schema, TableName, State)
    end;
do_register(update_schema, TableName, State = #lsm_engine_mgr_state{tables = SetOfTables, file = File}) ->
    NewSet = sets:add_element(TableName, SetOfTables),
    case dets:insert(File, {tables, NewSet}) of
        ok -> do_register(start_engine, TableName, State#lsm_engine_mgr_state{tables = NewSet});
        _ -> {{error, cant_write_schema}, State}
    end;
do_register(start_engine, TableName, State = #lsm_engine_mgr_state{tables = SetOfTables, file = File}) ->
    ChildSpec = #{
        id => TableName,
        start => {lsm_engine, start_link, [TableName]}
    },
    case supervisor:start_child(lsm_engine_sup, ChildSpec) of
        {ok, _} -> {ok, State};
        {ok, _, _} -> {ok, State};
        Err ->
            io:format("Cant start engine: ~p~n", [Err]),
            Set = sets:del_element(TableName, SetOfTables),
            dets:insert(File, {tables, Set}),
            {{error, cant_start_engine}, State#lsm_engine_mgr_state{tables = Set}}
    end.
