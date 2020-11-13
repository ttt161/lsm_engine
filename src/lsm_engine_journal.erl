%%%-------------------------------------------------------------------
%%% @author losto
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(lsm_engine_journal).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([
    get_init_cfg/1,
    rotation/0
]).

-include("lsm_engine.hrl").
-define(SERVER, ?MODULE).
-define(ROTATION_PERIOD, 300000). %% 5 minutes

-record(lsm_engine_journal_state, {dir, recovery, current, finished, current_path, finished_path}).

%%%===================================================================
%%% API
%%%===================================================================

get_init_cfg(TableName) ->
    gen_server:call(?MODULE, {get_init_cfg, TableName}).

rotation() ->
    gen_server:call(?MODULE, rotation).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    DbDir = lsm_engine_common:get_db_directory(),
    JrnDir = filename:join([DbDir, ?JOURNAL_DIR]),
    RecoveryList = parse_old_files(JrnDir),
    CurrentFile = new_journal(?CURRENT_PATTERN),
    CurrentPath = filename:join([JrnDir, CurrentFile]),
    {ok, CLog} = disk_log:open([{name, CurrentFile}, {file, CurrentPath}]),
    disk_log:log_terms(CLog, RecoveryList),
    FinishedFile = new_journal(?FINISHED_PATTERN),
    FinishedPath = filename:join([JrnDir, FinishedFile]),
    {ok, FLog} = disk_log:open([{name, FinishedFile}, {file, FinishedPath}]),
    RecByTable = lists:foldl(fun({{Table, OpId}, Operation}, Acc) ->
                                     Ops = maps:get(Table, Acc, []),
                                     Acc#{Table => [{OpId, Operation} | Ops]}
                             end, #{}, RecoveryList),
    timer:apply_after(?ROTATION_PERIOD, ?MODULE, rotation, []),
    {ok, #lsm_engine_journal_state{
            dir = JrnDir,
            recovery = RecByTable,
            current = CLog,
            finished = FLog,
            current_path = CurrentPath,
            finished_path = FinishedPath
           }}.
handle_call(rotation, _From, State = #lsm_engine_journal_state{
                                        dir = JrnDir,
                                        current = CLog,
                                        finished = FLog,
                                        current_path = CPath,
                                        finished_path = FPath
                                       }) ->

    CurrentFile = new_journal(?CURRENT_PATTERN),
    CurrentPath = filename:join([JrnDir, CurrentFile]),
    {ok, NewCLog} = disk_log:open([{name, CurrentFile}, {file, CurrentPath}]),
    FinishedFile = new_journal(?FINISHED_PATTERN),
    FinishedPath = filename:join([JrnDir, FinishedFile]),
    {ok, NewFLog} = disk_log:open([{name, FinishedFile}, {file, FinishedPath}]),
    Tables = lsm_engine_mgr:list(),
    lists:foreach(fun(Table) ->
                          lsm_engine:set_journals(Table, {FinishedPath, CurrentPath})
                  end, Tables),
    disk_log:sync(FLog),
    disk_log:sync(CLog),
    {_, FTerms} = disk_log:chunk(FLog, start, infinity),
    {_, CTerms} = disk_log:chunk(CLog, start, infinity),
    UnFin = lists:filter(fun({OpId, _Op}) -> not lists:member(OpId, FTerms) end, CTerms),
    ok = disk_log:log_terms(NewCLog, UnFin),
    disk_log:close(CLog),
    disk_log:close(FLog),
    file:delete(CPath),
    file:delete(FPath),
    {reply, ok, State#lsm_engine_journal_state{
                  current = NewCLog,
                  finished = NewFLog,
                  current_path = CurrentPath,
                  finished_path = FinishedPath
                 }};
handle_call({get_init_cfg, TableName}, _From, State = #lsm_engine_journal_state{
    recovery = RecMap,
    current_path = CPath,
    finished_path = FPath
}) ->

    RecOps = maps:get(TableName, RecMap, []),
    {reply, {FPath, CPath, RecOps}, State#lsm_engine_journal_state{
        recovery = maps:without([TableName], RecOps)
    }};
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

parse_old_files(JrnDir) ->
    {ok, ListFiles} = file:list_dir(JrnDir),
    {FinOpIds, CurrOps} = lists:foldl(fun(File, Acc) ->
                                              FilePath = filename:join([JrnDir, File]),
                                              case string:find(File, ?CURRENT_PATTERN) of
                                                  nomatch ->
                                                      case string:find(File, ?FINISHED_PATTERN) of
                                                          nomatch -> Acc;
                                                          _ -> parse(FilePath, Acc, finished)
                                                      end;
                                                  _ -> parse(FilePath, Acc, current)
                                              end
                                      end, {[], []}, ListFiles),
    lists:filter(fun({OpId, _Op}) -> not lists:member(OpId, FinOpIds) end, CurrOps).

parse(FilePath, {Fin, Cur}, Target) ->
    {ok, Log} = disk_log:open([{name, temp}, {file, FilePath}]),
    {_, Terms} = disk_log:chunk(Log, start, infinity),
    disk_log:close(Log),
    file:delete(FilePath),
    case Target of
        finished -> {lists:flatten([Terms, Fin]), Cur};
        current -> {Fin, lists:flatten([Terms, Cur])}
    end.

new_journal(Pattern) ->
    lists:flatten([Pattern, erlang:integer_to_list(erlang:monotonic_time())]).