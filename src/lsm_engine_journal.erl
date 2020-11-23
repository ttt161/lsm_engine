%%%-------------------------------------------------------------------
%%% @author losto
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(lsm_engine_journal).

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([
         get_init_cfg/1,
         rotation/1,
         do_rotation/3,
         read_terms/1,
         get_journals/0
        ]).

-include("lsm_engine.hrl").
-define(SERVER, ?MODULE).
%%

-record(lsm_engine_journal_state, {dir, recovery, current, finished, current_path, finished_path}).

%%%===================================================================
%%% API
%%%===================================================================

get_init_cfg(TableName) ->
    gen_server:call(?MODULE, {get_init_cfg, TableName}).

rotation(false) ->
    gen_server:call(?MODULE, rotation);
rotation(true) -> skip.

get_journals() ->
    gen_server:call(?MODULE, get_journals).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Dir) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Dir], []).

init([DbDir]) ->
    JrnDir = filename:join([DbDir, ?JOURNAL_DIR]),
    {RecoveryList, RecoveryMap} = parse_old_files(JrnDir),
    CurrentPath = filename:join([JrnDir, ?NEW_JOURNAL(?CURRENT_PATTERN)]),
    FinishedPath = filename:join([JrnDir, ?NEW_JOURNAL(?FINISHED_PATTERN)]),
    {ok, CurrentPath} = disk_log:open([{name, CurrentPath}, {file, CurrentPath}]),
    {ok, FinishedPath} = disk_log:open([{name, FinishedPath}, {file, FinishedPath}]),
    ok = disk_log:log_terms(CurrentPath, RecoveryList),
    ok = disk_log:sync(CurrentPath),
    {ok, #lsm_engine_journal_state{
            dir = JrnDir,
            recovery = RecoveryMap,
            current_path = CurrentPath,
            finished_path = FinishedPath
           }}.

handle_call(rotation, _From, State = #lsm_engine_journal_state{
                                        dir = JrnDir,
                                        current_path = CurrentPath,
                                        finished_path = FinishedPath
                                       }) ->

    %% create new CURRENT and FINISHED journals and notification all engines
    NewCurrentPath = filename:join([JrnDir, ?NEW_JOURNAL(?CURRENT_PATTERN)]),
    NewFinishedPath = filename:join([JrnDir, ?NEW_JOURNAL(?FINISHED_PATTERN)]),
    {ok, NewCurrentPath} = disk_log:open([{name, NewCurrentPath}, {file, NewCurrentPath}]),
    {ok, NewFinishedPath} = disk_log:open([{name, NewFinishedPath}, {file, NewFinishedPath}]),
    Tables = lsm_engine_mgr:list(),
    lists:foreach(fun(Table) ->
                          ok = lsm_engine:set_journals(Table, {NewFinishedPath, NewCurrentPath})
                  end, Tables),

    %% in separate process, parse old CURRENT and FINISHED journals,
    %% write unfinished operations into new CURRENT journal and delete old journals
    spawn(?MODULE, do_rotation, [CurrentPath, FinishedPath, NewCurrentPath]),
    {reply, ok, State#lsm_engine_journal_state{
                  current_path = NewCurrentPath,
                  finished_path = NewFinishedPath
                 }};
handle_call({get_init_cfg, TableName}, _From, State = #lsm_engine_journal_state{
                                                         recovery = RecMap,
                                                         current_path = CPath,
                                                         finished_path = FPath
                                                        }) ->

    RecOps = maps:get(TableName, RecMap, []),
    {reply, {FPath, CPath, RecOps}, State#lsm_engine_journal_state{
                                      recovery = maps:without([TableName], RecMap)
                                     }};
handle_call(get_journals, _From, State = #lsm_engine_journal_state{
                                            current_path = CPath,
                                            finished_path = FPath
                                           }) ->

    {reply, {CPath, FPath}, State};
handle_call(_Request, _From, State = #lsm_engine_journal_state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #lsm_engine_journal_state{}) ->
    {noreply, State}.

handle_info(_Info, State = #lsm_engine_journal_state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #lsm_engine_journal_state{}) ->
    ok.

code_change(_OldVsn, State = #lsm_engine_journal_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% TODO need refactor to read files by chunk
%%-spec(parse_old_files(JrnDir::string()) -> {
%%                                            RecoveryList::[{FullOperationId::{TableName::string(), LocalOperationId::ref()},
%%                                                            {OperationName::put, Record::{Key::term(), Value::term()}} |
%%                                                            {OperationName::delete, Key::term()}}],
%%                                            RecoveryMap::#{TableName::string() => [{LocalOperationId::ref(),
%%                                                                                    {put, {Key::term(), Value::term()}} |
%%                                                                                    {delete, Key::term()}}]}
%%                                           }).
parse_old_files(JrnDir) ->
    {ok, ListFiles} = file:list_dir(JrnDir),
    {FinOpIds, CurrOps} = lists:foldl(fun(File, Acc) ->
                                              FilePath = filename:join([JrnDir, File]),
                                              case string:find(File, ?CURRENT_PATTERN) of
                                                  nomatch ->
                                                      case string:find(File, ?FINISHED_PATTERN) of
                                                          nomatch ->
                                                              %%io:format("!!!unknown file: ~p~n", [File]),
                                                              Acc;
                                                          _ ->
                                                              FRes = parse(FilePath, Acc, finished),
                                                              FRes
                                                      end;
                                                  _ ->
                                                      CRes = parse(FilePath, Acc, current),
                                                      CRes
                                              end
                                      end, {[], []}, ListFiles),
    RecoveryList = lists:filter(fun({OpId, _Op}) -> not lists:member(OpId, FinOpIds) end, CurrOps),
    RecoveryMap = lists:foldl(fun({{Table, OpId}, Operation}, Acc) ->
                                      Ops = maps:get(Table, Acc, []),
                                      Acc#{Table => [{OpId, Operation} | Ops]}
                              end, #{}, RecoveryList),

    {RecoveryList, RecoveryMap}.

parse(FilePath, {Fin, Cur}, Target) ->
    {ok, Log} = disk_log:open([{name, temp}, {file, FilePath}]),
    Terms = read_terms(Log),
    disk_log:close(Log),
    file:delete(FilePath),
    case Target of
        finished -> {lists:flatten([Terms, Fin]), Cur};
        current -> {Fin, lists:flatten([Terms, Cur])}
    end.

read_terms(Log) ->
    case disk_log:chunk(Log, start, infinity) of
        {_, List} -> List;
        eof -> []
    end.

do_rotation(CurrentPath, FinishedPath, NewCurrentPath) ->
    ok = disk_log:sync(FinishedPath),
    ok = disk_log:sync(CurrentPath),
    FTerms = read_terms(FinishedPath),
    CTerms = read_terms(CurrentPath),
    io:format("cterms ~p~n", [CTerms]),
    io:format("fterms ~p~n", [FTerms]),
    UnFin = lists:filter(fun({OpId, _Op}) -> not lists:member(OpId, FTerms) end, CTerms),
    io:format("unfin ~p", [UnFin]),
    ok = disk_log:log_terms(NewCurrentPath, UnFin),
    ok = disk_log:sync(NewCurrentPath),
    ok = disk_log:close(CurrentPath),
    ok = disk_log:close(FinishedPath),
    ok = file:delete(CurrentPath),
    ok = file:delete(FinishedPath).
    %timer:apply_after(?ROTATION_PERIOD, ?MODULE, rotation, [?TEST]).
