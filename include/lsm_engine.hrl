%%%-------------------------------------------------------------------
%%% @author losto
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. нояб. 2020 12:58
%%%-------------------------------------------------------------------
-author("losto").

-define(TABLE(Name),    try
                            erlang:list_to_existing_atom(Name)
                        catch
                            _:_  -> erlang:list_to_atom(Name)
                        end ).
-define(DEFAULT_DB_DIR,             "/tmp/edb").
-define(DEFAULT_MAX_RAM_TABLE,      4194304).   % 4 MB
-define(DEFAULT_MAX_FILE_SEGMENT,   67108864).  % 64 MB
-define(INFO_FILE, "t_info").
-define(SCHEMA_FILE, "schema").
-define(JOURNAL_DIR, "journal").
-define(CURRENT_PATTERN, "current").
-define(FINISHED_PATTERN, "finished").