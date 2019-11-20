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
%% For RPC
-export([remove_local_handles/1]).

-define(FILE_HANDLES_TREE_ID, <<"storage_file_handles">>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Add link to handle.
%% @end
%%--------------------------------------------------------------------
-spec add(SessId :: session:id(), HandleId :: storage_driver:handle_id(),
    Handle :: storage_driver:handle()) -> ok | {error, term()}.
add(SessId, HandleId, Handle) ->
    case sd_handle:create(#document{value = Handle}) of
        {ok, Key} ->
            session:add_local_links(SessId, ?FILE_HANDLES_TREE_ID, HandleId, Key);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Remove link to handle.
%% @end
%%--------------------------------------------------------------------
-spec remove(SessId :: session:id(), HandleId :: storage_driver:handle_id()) ->
    ok | {error, term()}.
remove(SessId, HandleId) ->
    case session:get_local_link(SessId, ?FILE_HANDLES_TREE_ID, HandleId) of
        {ok, [#link{target = HandleKey}]} ->
            case sd_handle:delete(HandleKey) of
                ok ->
                    session:delete_local_links(SessId, ?FILE_HANDLES_TREE_ID, HandleId);
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
-spec get(SessId :: session:id(), HandleId :: storage_driver:handle_id()) ->
    {ok, storage_driver:handle()} | {error, term()}.
get(SessId, HandleId) ->
    case session:get_local_link(SessId, ?FILE_HANDLES_TREE_ID, HandleId) of
        {ok, [#link{target = HandleKey}]} ->
            case sd_handle:get(HandleKey) of
                {ok, #document{value = Handle}} ->
                    {ok, Handle};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes all associated sd handles.
%% @end
%%--------------------------------------------------------------------
-spec remove_handles(SessId :: session:id()) -> ok.
remove_handles(SessId) ->
    {AnsList, []} = rpc:multicall(consistent_hashing:get_all_nodes(), ?MODULE, remove_local_handles, [SessId]),
    lists:foreach(fun(Ans) -> ok = Ans end, AnsList).


%%--------------------------------------------------------------------
%% @doc
%% Removes all associated sd handles on node.
%% @end
%%--------------------------------------------------------------------
-spec remove_local_handles(SessId :: session:id()) -> ok.
remove_local_handles(SessId) ->
    {ok, Links} = session:fold_local_links(SessId, ?FILE_HANDLES_TREE_ID,
        fun(Link = #link{}, Acc) -> {ok, [Link | Acc]} end
    ),
    Names = lists:map(fun(#link{name = Name, target = HandleKey}) ->
        sd_handle:delete(HandleKey),
        Name
    end, Links),
    session:delete_local_links(SessId, ?FILE_HANDLES_TREE_ID, Names),
    ok.