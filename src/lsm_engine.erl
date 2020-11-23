%%%-------------------------------------------------------------------
%%% @author losto
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(lsm_engine).

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([
         table_create/2,
         table_delete/1,
         put/2,
         set_journals/2
        ]).

-include("lsm_engine.hrl").
-define(SERVER, ?MODULE).

-record(lsm_engine_state, {
                           table,
                           name,
                           type,
                           dir,
                           info,
                           max_ram_size,
                           max_file_size,
                           operations,
                           journal_current,
                           journal_finished,
                           size,
                           ranges
                          }).

%%%===================================================================
%%% API
%%%===================================================================
-spec(table_create(TableName::string(), TableType::disk|ram) -> ok | {error, Reason::term()}).
table_create(TableName, TableType) ->
    case internal_table_create(TableName, TableType) of
        ok ->
            case lsm_engine_mgr:register(TableName) of
                ok -> ok;
                Error ->
                    internal_table_delete(TableName),
                    Error
            end;
        Error -> Error
    end.

table_delete(TableName) ->
    case lsm_engine_mgr:unregister(TableName) of
        ok -> internal_table_delete(TableName);
        Error -> Error
    end.

put(TableName, Record) when is_tuple(Record) ->
    ?MODULE:put(TableName, [Record]);
put(TableName, RecordList) ->
    case catch gen_server:call(?TABLE(TableName), {put, RecordList}) of
        {ok, OpId} -> {ok, OpId};
        {error, Error} -> {error, Error};
        Other -> {error, Other}
    end.
%get() -> ok.
%delete() -> ok.

set_journals(TableName, {LogFinished, LogCurrent}) ->
    case catch gen_server:call(?TABLE(TableName), {set_journals, LogFinished, LogCurrent}) of
        ok -> ok;
        _Other -> ok
    end.

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(TableName) ->
    Table = ?TABLE(TableName),
    gen_server:start_link({local, Table}, ?MODULE, [Table, TableName], []).

init([_Table, TableName]) ->
    DbDir = lsm_engine_common:get_db_directory(),
    MaxRam = get_max_ram(),
    MaxFile = get_max_file(),
    TableDir = filename:join(DbDir, TableName),
    TableInfo = open_t_info(TableName, TableDir),
    {TableType, Ranges} = read_t_info(TableInfo),
    Tab = new_mem_table(),
    {FPath, CPath, RecOps} = lsm_engine_journal:get_init_cfg(TableName),
    {ok, FPath} = disk_log:open([{name, FPath}, {file, FPath}]),
    {ok, CPath} = disk_log:open([{name, CPath}, {file, CPath}]),
    {OpIds, Size} = lists:foldl(fun
                                    ({OpId, {put, Record}} , {Ops, Sz}) ->
                                       true = ets:insert(Tab, Record),
                                       { [{TableName, OpId} | Ops], Sz + erlang:size(term_to_binary(Record)) };
                                    ({OpId, {delete, Key}}, {Ops, Sz}) ->
                                       true = ets:insert(Tab, {Key, deleted}),
                                       { [{TableName, OpId} | Ops], Sz }
                               end, {[], 0}, RecOps),
    State = #lsm_engine_state{
        table = Tab,
        name = TableName,
        type = TableType,
        dir = TableDir,
        info = TableInfo,
        max_ram_size = MaxRam,
        max_file_size = MaxFile,
        operations = OpIds,
        journal_finished = FPath,
        journal_current = CPath,
        size = Size,
        ranges = Ranges},
    {ok, State}.

handle_call(_Operation = {put, Records}, _From, State) ->

    {Result, NewState} = case catch do_put(check_records, Records, State) of
                             {{ok, OpId}, NewSt} -> {{ok, OpId}, NewSt};
                             {error,Other} -> {{error, Other}, State};
                             Other -> {{error, Other}, State}
                         end,

    {reply, Result, NewState};
handle_call({set_journals, LogFinished, LogCurrent}, _From, State =
    #lsm_engine_state{journal_finished = FLog, journal_current = CLog}) ->
    disk_log:close(FLog),
    disk_log:close(CLog),
    disk_log:open([{name, LogFinished},{file, LogFinished}]),
    disk_log:open([{name, LogCurrent},{file, LogCurrent}]),
    {reply, ok, State#lsm_engine_state{journal_current = LogCurrent, journal_finished = LogFinished}};
handle_call(_Request, _From, State = #lsm_engine_state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #lsm_engine_state{}) ->
    {noreply, State}.

handle_info(_Info, State = #lsm_engine_state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #lsm_engine_state{}) ->
    ok.

code_change(_OldVsn, State = #lsm_engine_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


get_max_ram() ->
    application:get_env(lsm_engine, max_ram_table, ?DEFAULT_MAX_RAM_TABLE).
get_max_file() ->
    application:get_env(lsm_engine, max_file_segment, ?DEFAULT_MAX_FILE_SEGMENT).
open_t_info(TableName, TableDir) ->
    File = filename:join(TableDir, ?INFO_FILE),
    {ok, Name} = dets:open_file(TableName, [{file, File}]),
    Name.
read_t_info(InfoFile) ->
    TType = case dets:lookup(InfoFile, t_type) of
                [{t_type, TRes}] -> TRes;
                [] -> dets:insert(InfoFile, {t_type, disk})
            end,
    Ranges = case dets:lookup(InfoFile, ranges) of
                 [{ranges, RRes}] -> RRes;
                 [] ->
                     dets:insert(InfoFile, {ranges, #{}}),
                     #{}
             end,
    {TType, Ranges}.
update_t_info(InfoFile, Key, Value) ->
    dets:insert(InfoFile, {Key, Value}).

internal_table_create(TableName, TableType) when TableType =:= disk orelse TableType =:= ram ->
    DbDir = lsm_engine_common:get_db_directory(),
    TableDir = filename:join(DbDir, TableName),
    InfoFile = filename:join(TableDir, ?INFO_FILE),
    case file:make_dir(TableDir) of
        ok ->
            try
                begin
                    {ok, Name} = dets:open_file(TableName, [{file, InfoFile}]),
                    ok = dets:insert(Name, [{t_type, TableType}]),
                    ok = dets:close(Name),
                    ok
                end
            catch
                _:Err  ->
                    {error, Err}
            end;
        Error -> Error
    end;
internal_table_create(_TableName, TableType) -> {error, {bad_table_type, TableType}}.

internal_table_delete(TableName) ->
    DbDir = lsm_engine_common:get_db_directory(),
    TableDir = filename:join(DbDir, TableName),
    ListFiles = case file:list_dir(TableDir) of
                          {ok, List} -> List;
                          {error, _Reason} ->
                              %%io:format("cant read list files from dir ~p with reason ~p~n", [TableDir, Reason]),
                              []
                      end,
    lists:foreach(fun(File) ->
                          file:delete(filename:join([TableDir, File]))
                  end, ListFiles),
    file:del_dir(TableDir).

do_put(check_records, Records, State) ->
    case lists:filter(fun(Rec) -> not (is_tuple(Rec) andalso erlang:size(Rec) =:= 2) end, Records) of
        [] -> do_put(insert, Records, State);
        BadRecords -> throw({bad_records,BadRecords})
    end;
do_put(insert, Records, State = #lsm_engine_state{
                                   table = Tab,
                                   name = TableName,
                                   operations = Ops,
                                   journal_current = CLog,
                                   size = TSize}) ->
    OpId = {TableName, make_ref()},
    ok = disk_log:log_terms(CLog, [{OpId, {put, Records}}]),
    ok = disk_log:sync(CLog),
    true = ets:insert(Tab, Records),
    do_put(dump_file, OpId, State#lsm_engine_state{operations = [OpId | Ops],
                                                                     size = TSize + erlang:size(term_to_binary(Records))});
do_put(dump_file, OpId, State = #lsm_engine_state{
                                        table = Tab,
                                        type = TableType,
                                        dir = TableDir,
                                        max_ram_size = MaxRam,
                                        info = TableInfo,
                                        operations = Ops,
                                        journal_finished = FLog,
                                        size = TSize,
                                        ranges = Ranges}) when TSize >= MaxRam andalso TableType =:= disk ->
    %io:format("dumping table~n"),
    File = filename:join([TableDir, new_file("unmerge_")]),
    ok = ets:tab2file(Tab, File),
    ok = disk_log:log_terms(FLog, Ops),
    Range = {ets:first(Tab), ets:last(Tab)},
    NewRanges = Ranges#{Range => File},
    update_t_info(TableInfo, ranges, NewRanges),
    ets:delete(Tab),
    NewTab = new_mem_table(),
    {{ok, OpId}, State#lsm_engine_state{table = NewTab, operations = [], size = 0, ranges = NewRanges}};
do_put(dump_file, OpId, State) ->
    {{ok, OpId}, State}.


new_file(Pattern) ->
    lists:flatten([Pattern, erlang:integer_to_list(erlang:monotonic_time())]).

new_mem_table() ->
    ets:new(memTable, [ordered_set, private]).