%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module for storing of file handles in session.
%%% @end
%%%-------------------------------------------------------------------
-module(session_handles).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% API
-export([add/3, remove/2, get/2, remove_handles/1]).

-define(FILE_HANDLES_TREE_ID, <<"storage_file_handles">>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Add link to handle.
%% @end
%%--------------------------------------------------------------------
-spec add(SessId :: session:id(), HandleId :: storage_file_manager:handle_id(),
    Handle :: storage_file_manager:handle()) -> ok | {error, term()}.
add(SessId, HandleId, Handle) ->
    case sfm_handle:create(#document{value = Handle}) of
        {ok, Key} ->
            session:add_links(SessId, ?FILE_HANDLES_TREE_ID, HandleId, Key);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Remove link to handle.
%% @end
%%--------------------------------------------------------------------
-spec remove(SessId :: session:id(), HandleId :: storage_file_manager:handle_id()) ->
    ok | {error, term()}.
remove(SessId, HandleId) ->
    case session:get_link(SessId, ?FILE_HANDLES_TREE_ID, HandleId) of
        {ok, [#link{target = HandleKey}]} ->
            case sfm_handle:delete(HandleKey) of
                ok ->
                    session:delete_links(SessId, ?FILE_HANDLES_TREE_ID, HandleId);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets handle.
%% @end
%%--------------------------------------------------------------------
-spec get(SessId :: session:id(), HandleId :: storage_file_manager:handle_id()) ->
    {ok, storage_file_manager:handle()} | {error, term()}.
get(SessId, HandleId) ->
    case session:get_link(SessId, ?FILE_HANDLES_TREE_ID, HandleId) of
        {ok, [#link{target = HandleKey}]} ->
            case sfm_handle:get(HandleKey) of
                {ok, #document{value = Handle}} ->
                    {ok, Handle};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

remove_handles(SessId) ->
    {ok, Links} = session:fold_links(SessId, ?FILE_HANDLES_TREE_ID,
        fun(Link = #link{}, Acc) -> {ok, [Link | Acc]} end
    ),
    Names = lists:map(fun(#link{name = Name, target = HandleKey}) ->
        sfm_handle:delete(HandleKey),
        Name
    end, Links),
    session:delete_links(SessId, ?FILE_HANDLES_TREE_ID, Names),
    ok.