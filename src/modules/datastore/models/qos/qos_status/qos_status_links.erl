%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%% Module responsible for managing QoS status links required for status calculation.
%%% For more details consult `qos_status` module doc.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_status_links).
-author("Michal Stanisz").

-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    add_synced_link/3, delete_synced_link/3,
    delete_all_local_links_with_prefix/3, 
    get_next_status_links/4
]).

-type id() :: datastore_doc:key().

-define(CTX, (qos_status:get_ctx())).
-define(LIST_LINKS_BATCH_SIZE, 20).

%%%===================================================================
%%% API
%%%===================================================================

-spec add_synced_link(datastore_doc:scope(), datastore:key(), {datastore:link_name(), 
    datastore:link_target()}) -> {ok, datastore:link()} | {error, term()}.
add_synced_link(SpaceId, Key, Link) ->
    datastore_model:add_links(?CTX#{scope => SpaceId}, Key, oneprovider:get_id(), Link).

-spec delete_synced_link(datastore_doc:scope(), datastore:key(), datastore:link_name()
    | {datastore:link_name(), datastore:link_rev()}) -> ok | {error, term()}.
delete_synced_link(SpaceId, Key, Link) ->
    ok = datastore_model:delete_links(?CTX#{scope => SpaceId}, Key, oneprovider:get_id(), Link).


-spec delete_all_local_links_with_prefix(od_space:id(), datastore:key(), qos_status:path()) -> ok.
delete_all_local_links_with_prefix(SpaceId, Key, Prefix) ->
    case get_next_status_links(Key, Prefix, ?LIST_LINKS_BATCH_SIZE, oneprovider:get_id()) of
        {ok, []} -> ok;
        {ok, Links} ->
            case delete_links_with_prefix_in_batch(SpaceId, Key, Prefix, Links) of
                finished -> ok;
                false -> delete_all_local_links_with_prefix(SpaceId, Key, Prefix)
            end
    end.


-spec get_next_status_links(datastore:key(), qos_status:path(), non_neg_integer(), datastore_links:tree_ids()) ->
    {ok, [qos_status:path()]}.
get_next_status_links(Key, Path, BatchSize, TreeId) ->
    fold_links(Key, TreeId,
        fun(#link{name = Name}, Acc) -> {ok, [Name | Acc]} end,
        [],
        #{prev_link_name => Path, size => BatchSize}
    ).

%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec fold_links(id(), datastore_model:tree_ids(), datastore:fold_fun(datastore:link()),
    datastore:fold_acc(), datastore:fold_opts()) -> {ok, datastore:fold_acc()} |
    {{ok, datastore:fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_links(Key, TreeIds, Fun, Acc, Opts) ->
    datastore_model:fold_links(?CTX, Key, TreeIds, Fun, Acc, Opts).


%% @private
-spec delete_links_with_prefix_in_batch(od_space:id(), datastore:key(), qos_status:path(), 
    [datastore:link_name()]) -> finished | false.
delete_links_with_prefix_in_batch(SpaceId, Key, Prefix, Links) ->
    lists:foldl(fun 
        (_, finished) -> finished;
        (LinkName, _) ->
            case str_utils:binary_starts_with(LinkName, Prefix) of
                true ->
                    ok = delete_synced_link(SpaceId, Key, LinkName),
                    false;
                false -> 
                    finished
            end
        end, false, Links).
