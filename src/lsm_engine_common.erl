%%%-------------------------------------------------------------------
%%% @author losto
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. нояб. 2020 12:50
%%%-------------------------------------------------------------------
-module(lsm_engine_common).
-author("losto").

-include("lsm_engine.hrl").
%% API
-export([
    get_db_directory/0
]).

get_db_directory() ->
    application:get_env(lsm_engine, db_dir, ?DEFAULT_DB_DIR).
