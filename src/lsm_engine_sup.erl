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
    JournalSpec = #{
        id => journal,
        start => {lsm_engine_journal, start_link, []}
    },
    MgrSpec = #{
        id => manager,
        start => {lsm_engine_mgr, start_link, [JournalSpec, MgrSpec]}
    },
    {ok, {{one_for_all, 0, 1}, []}}.

%%====================================================================
%% Internal functions
%%====================================================================
