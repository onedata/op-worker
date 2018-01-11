%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for caching a local helper handle.
%%% @end
%%%-------------------------------------------------------------------
-module(helper_handle).
-author("Konrad Zemek").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([create/4, get/1, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type id() :: datastore:key().
-type record() :: #helper_handle{}.
-type doc() :: datastore_doc:doc(record()).
-export_type([doc/0]).

-define(CTX, #{
    model => ?MODULE,
    routing => local,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates and caches helper handle.
%% @end
%%--------------------------------------------------------------------
-spec create(session:id(), od_user:id(), od_space:id(), storage:doc()) ->
    {ok, doc()}.
create(SessionId, UserId, SpaceId, StorageDoc) ->
    {ok, Helper} = fslogic_storage:select_helper(StorageDoc),
    HelperName = helper:get_name(Helper),
    {ok, UserCtx} = luma:get_server_user_ctx(
        SessionId, UserId, undefined, SpaceId, StorageDoc, HelperName
    ),
    HelperHandle = helpers:get_helper_handle(Helper, UserCtx),
    HelperDoc = #document{value = HelperHandle},
    datastore_model:create(?CTX, HelperDoc).

%%--------------------------------------------------------------------
%% @doc
%% Returns helper handle by ID.
%% @end
%%--------------------------------------------------------------------
-spec get(id()) -> {ok, doc()} | {error, term()}.
get(HandleId) ->
    datastore_model:get(?CTX, HandleId).

%%--------------------------------------------------------------------
%% @doc
%% Deletes helper handle by ID.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(HandleId) ->
    datastore_model:delete(?CTX, HandleId).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.