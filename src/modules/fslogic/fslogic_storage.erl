%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module for storage management.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_storage).
-author("Rafal Slota").

-include("cluster/worker/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([select_helper/1, select_storage/1, new_storage/2, new_helper_init/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns any available storage for given fslogic ctx.
%% @end
%%--------------------------------------------------------------------
-spec select_helper(datastore:document() | #storage{}) -> {ok, #helper_init{}} | {error, Reason :: term()}.
select_helper(#document{value = Storage}) ->
    select_helper(Storage);
select_helper(#storage{helpers = []} = Storage) ->
    {error, {no_helper_available, Storage}};
select_helper(#storage{helpers = [Helper | _]}) ->
    {ok, Helper}.


%%--------------------------------------------------------------------
%% @doc
%% Returns any available storage for given fslogic ctx.
%% @end
%%--------------------------------------------------------------------
-spec select_storage(fslogic_worker:ctx()) -> {ok, datastore:document()} | {error, Reason :: term()}.
select_storage(#fslogic_ctx{}) ->
    %% For now just return any available storage.
    case storage:list() of
        {ok, [#document{} = Storage | _]} ->
            {ok, Storage};
        {ok, []} ->
            {error, {not_found, storage}};
        E -> E
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates new helper_init structure.
%% @end
%%--------------------------------------------------------------------
-spec new_helper_init(HelperName :: helpers:name(), HelperArgs :: helpers:args()) -> #helper_init{}.
new_helper_init(HelperName, HelperArgs) ->
    #helper_init{name = HelperName, args = HelperArgs}.


%%--------------------------------------------------------------------
%% @doc
%% Creates new storage structure.
%% @end
%%--------------------------------------------------------------------
-spec new_storage(Name :: storage:name(), [#helper_init{}]) -> #storage{}.
new_storage(Name, Helpers) ->
    #storage{name = Name, helpers = Helpers}.

%%%===================================================================
%%% Internal functions
%%%===================================================================