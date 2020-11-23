%%%-------------------------------------------------------------------
%% @doc lsm_engine top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(lsm_engine_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-include("lsm_engine.hrl").

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: #{id => Id, start => {M, F, A}}
%% Optional keys are restart, shutdown, type, modules.
%% Before OTP 18 tuples must be used to specify a child. e.g.
%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    Dir = get_db_directory(),
    JournalSpec = #{
        id => journal,
        start => {lsm_engine_journal, start_link, [Dir]}
    },
    MgrSpec = #{
        id => manager,
        start => {lsm_engine_mgr, start_link, [Dir]}
    },
    {ok, {{one_for_all, 0, 1}, [JournalSpec, MgrSpec]}}.

%%====================================================================
%% Internal functions
%%====================================================================

get_db_directory() ->
    application:get_env(lsm_engine, db_dir, ?DEFAULT_DB_DIR).