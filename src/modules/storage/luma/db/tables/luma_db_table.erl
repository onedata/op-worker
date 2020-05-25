%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines behaviour that must be implemented by all
%%% modules that are used as LUMA DB tables.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_db_table).
-author("Jakub Kudzia").


%%%===================================================================
%%% Callback definitions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function will be called to acquire specific luma_db:db_record()
%% for given luma_db:table() that will stored in the LUMA DB.
%% @end
%%--------------------------------------------------------------------
-callback acquire(storage:data(), luma_db:db_key()) ->
    {ok, luma_db:db_record()} | {error, term()}.