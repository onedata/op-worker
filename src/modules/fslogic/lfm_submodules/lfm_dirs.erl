%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%% @doc This module performs directory-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_dirs).

-include("types.hrl").
-include("errors.hrl").

%% API
-export([mkdir/1, ls/3, get_children_count/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec mkdir(Path :: file_path()) -> {ok, file_uuid()} | error_reply().
mkdir(_Path) ->
    {ok, <<"">>}.


%%--------------------------------------------------------------------
%% @doc
%% Lists some contents of a directory.
%% Returns up to Limit of entries, starting with Offset-th entry.
%%
%% @end
%%--------------------------------------------------------------------
-spec ls(FileKey :: file_id_or_path(), Limit :: integer(), Offset :: integer()) -> {ok, [{file_uuid(), file_name()}]} | error_reply().
ls(_FileKey, _Limit, _Offset) ->
    {ok, []}.


%%--------------------------------------------------------------------
%% @doc
%% Returns number of children of a directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_children_count(FileKey :: file_id_or_path()) -> {ok, integer()} | error_reply().
get_children_count(_FileKey) ->
    {ok, 0}.