%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(session_helpers).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% API
-export([get_helper/3, delete_helpers_on_this_node/1]).

-define(HELPER_HANDLES_TREE_ID, <<"helper_handles">>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves a helper associated with the session by
%% {SessId, SpaceUuid} key. The helper is created and associated
%% with the session if it doesn't exist.
%% @end
%%--------------------------------------------------------------------
-spec get_helper(session:id(), od_space:id(), storage:doc()) ->
    {ok, helpers:helper_handle()} | {error, term()}.
get_helper(SessId, SpaceId, StorageDoc) ->
    get_helper(SessId, SpaceId, StorageDoc, false).

%%--------------------------------------------------------------------
%% @doc
%% Removes all associated helper handles present on the node.
%% @end
%%--------------------------------------------------------------------
-spec delete_helpers_on_this_node(SessId :: session:id()) ->
    ok | {error, term()}.
delete_helpers_on_this_node(SessId) ->
    {ok, Links} = session:fold_local_links(SessId, ?HELPER_HANDLES_TREE_ID,
        fun(Link = #link{}, Acc) -> {ok, [Link | Acc]} end
    ),
    Names = lists:map(fun(#link{name = Name, target = HandleId}) ->
        helper_handle:delete(HandleId),
        Name
    end, Links),
    session:delete_local_links(SessId, ?HELPER_HANDLES_TREE_ID, Names),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Attempts to fetch a helper handle through link API. If fetching
%% fails with enoent, enters the critical section and retries the
%% request, then inserts a new helper handle if the helper is still missing.
%% The first, out-of-critical-section fetch is an optimization.
%% The fetch+insert occurs in the critical section to avoid
%% instantiating unnecessary helper handles.
%% @end
%%--------------------------------------------------------------------
-spec get_helper(session:id(), od_space:id(), storage:doc(),
    InCriticalSection :: boolean()) ->
    {ok, helpers:helper_handle()} | {error, term()}.
get_helper(SessId, SpaceId, StorageDoc, InCriticalSection) ->
    StorageId = storage:get_id(StorageDoc),
    FetchResult = case session:get_local_links(SessId,
        ?HELPER_HANDLES_TREE_ID, link_key(StorageId, SpaceId)) of
        {ok, [#link{target = Key}]} ->
            helper_handle:get(Key);
        {error, not_found} ->
            {error, link_not_found};
        {error, Reason} ->
            {error, Reason}
    end,
    case {FetchResult, InCriticalSection} of
        {{ok, #document{value = Handle}}, _} ->
            {ok, Handle};

        {{error, link_not_found}, false} ->
            critical_section:run({SessId, SpaceId, StorageId}, fun() ->
                get_helper(SessId, SpaceId, StorageDoc, true)
            end);

        {{error, link_not_found}, true} ->
            add_missing_helper(SessId, SpaceId, StorageDoc);

        {{error, not_found}, false} ->
            critical_section:run({SessId, SpaceId, StorageId}, fun() ->
                get_helper(SessId, SpaceId, StorageDoc, true)
            end);

        {{error, not_found}, true} ->
            %todo this is just temporary fix, VFS-4301
            LinkKey = link_key(StorageId, SpaceId),
            session:delete_local_links(SessId, ?HELPER_HANDLES_TREE_ID, LinkKey),
            add_missing_helper(SessId, SpaceId, StorageDoc);

        {Error2, _} ->
            {error, Error2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a new #helper_handle{} document in the database and links
%% it with current session.
%% @end
%%--------------------------------------------------------------------
-spec add_missing_helper(session:id(), od_space:id(), storage:doc()) ->
    {ok, helpers:helper_handle()} | {error, term()}.
add_missing_helper(SessId, SpaceId, StorageDoc) ->
    StorageId = storage:get_id(StorageDoc),
    {ok, UserId} = session:get_user_id(SessId),

    {ok, #document{key = HandleId, value = HelperHandle}} =
        helper_handle:create(SessId, UserId, SpaceId, StorageDoc),

    case session:add_local_links(SessId, ?HELPER_HANDLES_TREE_ID,
        link_key(StorageId, SpaceId), HandleId
    ) of
        {ok, _} ->
            {ok, HelperHandle};
        {error, Reason} ->
            helper_handle:delete(HandleId),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a key constructed from StorageId and SpaceUuid used for
%% link targets.
%% @end
%%--------------------------------------------------------------------
-spec link_key(StorageId :: storage:id(), SpaceUuid :: file_meta:uuid()) ->
    binary().
link_key(StorageId, SpaceUuid) ->
    <<StorageId/binary, ":", SpaceUuid/binary>>.