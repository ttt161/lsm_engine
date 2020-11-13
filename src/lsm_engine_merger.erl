%%%-------------------------------------------------------------------
%%% @author losto
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(lsm_engine_merger).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(lsm_engine_merger_state, {}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #lsm_engine_merger_state{}}.

handle_call(_Request, _From, State = #lsm_engine_merger_state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #lsm_engine_merger_state{}) ->
    {noreply, State}.

handle_info(_Info, State = #lsm_engine_merger_state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #lsm_engine_merger_state{}) ->
    ok.

code_change(_OldVsn, State = #lsm_engine_merger_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
