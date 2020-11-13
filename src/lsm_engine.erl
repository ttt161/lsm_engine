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
                           journal_finished
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

%% TODO need protection from noproc error
put(TableName, Record = {_Key, _Value}) ->
    gen_server:call(?TABLE(TableName), {put, Record});
put(_, Record) ->
    {error, {bad_record, Record}}.
get() -> ok.
delete() -> ok.

set_journals(TableName, {LogFinished, LogCurrent}) -> ok.

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(TableName) ->
    Table = ?TABLE(TableName),
    gen_server:start_link({local, Table}, ?MODULE, [Table, TableName], []).

init([Table, TableName]) ->
    DbDir = lsm_engine_common:get_db_directory(),
    MaxRam = get_max_ram(),
    MaxFile = get_max_file(),
    TableDir = filename:join(DbDir, TableName),
    TableInfo = read_t_info(TableName, TableDir), %% TODO this unsafe
    TableType = maps:get(t_type, TableInfo, disk),
    Tab = ets:new(Table, [ordered_set, private]),
    {FPath, CPath, RecOps} = lsm_engine_journal:get_init_cfg(TableName),
    disk_log:open([{name, FPath}, {file, FPath}]),
    disk_log:open([{name, CPath}, {file, CPath}]),
    OpIds = lists:foldl(fun
                            ({OpId, {put, Record}} ,Acc) ->
                                true = ets:insert(Tab, Record),
                                [{TableName, OpId} | Acc];
                            ({OpId, {delete, Key}}, Acc) ->
                                true = ets:insert(Tab, {Key, deleted}),
                                [{TableName, OpId} | Acc]
                         end, [], RecOps),
    {ok, #lsm_engine_state{
            table = Tab,
            name = TableName,
            type = TableType,
            dir = TableDir,
            info = TableInfo,
            max_ram_size = MaxRam,
            max_file_size = MaxFile,
            operations = OpIds,
            journal_finished = FPath,
            journal_current = CPath}
    }.

handle_call(Operation = {put, Record}, _From, State = #lsm_engine_state{
                                                         table = Tab,
                                                         name = TableName,
                                                         type = TableType,
                                                         dir = TableDir,
                                                         max_ram_size = MaxRam,
                                                         info = TableInfo,
                                                         operations = Ops}) ->

    Result0 = registering(Operation),


    {reply, ok, State};

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
    application:get_env(max_ram_table, ?DEFAULT_MAX_RAM_TABLE).
get_max_file() ->
    application:get_env(max_file_segment, ?DEFAULT_MAX_FILE_SEGMENT).
read_t_info(TableName, TableDir) ->
    File = filename:join(TableDir, ?INFO_FILE),
    {ok, Name} = dets:open_file(TableName, [{file, File}]),
    dets:foldl(fun({Key, Value}, Acc) -> Acc#{Key => Value} end, #{}, Name).

internal_table_create(TableName, TableType) ->
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
    end.

internal_table_delete(TableName) ->
    DbDir = lsm_engine_common:get_db_directory(),
    TableDir = filename:join(DbDir, TableName),
    ListFiles = file:list_dir(TableDir),
    lists:foreach(fun(File) ->
                          file:delete(filename:join([TableDir, File]))
                  end, ListFiles),
    file:del_dir(TableDir).
