%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%% Module responsible for managing QoS status links required for status calculation. 
%%% All links are synced between providers.
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
    add_link/2, delete_link/2,
    delete_all_local_links_with_prefix/2,
    get_next_local_links/3
]).

-type key() :: binary().
-type tree_ids() :: all | oneprovider:id().
-type link_name() :: qos_status:path().
-type link_value() :: binary().
-type link() :: {link_name(), link_value()}.

-define(CTX, (qos_status:get_ctx())).
-define(LIST_LINKS_BATCH_SIZE, 20).

%%%===================================================================
%%% API
%%%===================================================================

-spec add_link(key(), link()) -> ok | {error, term()}.
add_link(Key, Link) ->
    ?extract_ok(?ok_if_exists(datastore_model:add_links(?CTX, Key, oneprovider:get_id(), Link))).


-spec delete_link(key(), link_name()) -> ok | {error, term()}.
delete_link(Key, Link) ->
    ok = datastore_model:delete_links(?CTX, Key, oneprovider:get_id(), Link).


-spec delete_all_local_links_with_prefix(key(), qos_status:path()) -> ok.
delete_all_local_links_with_prefix(Key, Prefix) ->
    case get_next_local_links(Key, Prefix, ?LIST_LINKS_BATCH_SIZE) of
        {ok, []} -> ok;
        {ok, Links} ->
            case delete_links_with_prefix_in_batch(Key, Prefix, Links) of
                finished -> ok;
                not_finished -> delete_all_local_links_with_prefix(Key, Prefix)
            end
    end.


-spec get_next_local_links(key(), qos_status:path(), non_neg_integer()) ->
    {ok, [qos_status:path()]}.
get_next_local_links(Key, Path, BatchSize) ->
    {ok, ReversedLinks} = fold_local_links(
        Key,
        fun(#link{name = Name}, Acc) -> {ok, [Name | Acc]} end,
        [],
        #{prev_link_name => Path, size => BatchSize}
    ),
    {ok, lists:reverse(ReversedLinks)}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec fold_local_links(key(), datastore:fold_fun(datastore:link()),
    datastore:fold_acc(), datastore:fold_opts()) -> {ok, datastore:fold_acc()} |
    {{ok, datastore:fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_local_links(Key, Fun, Acc, Opts) ->
    datastore_model:fold_links(?CTX, Key, oneprovider:get_id(), Fun, Acc, Opts).


%% @private
-spec delete_links_with_prefix_in_batch(key(), qos_status:path(), [link_name()]) ->
    finished | not_finished.
delete_links_with_prefix_in_batch(Key, Prefix, Links) ->
    lists:foldl(fun 
        (_, finished) -> finished;
        (LinkName, _) ->
            case str_utils:binary_starts_with(LinkName, Prefix) of
                true ->
                    ok = delete_link(Key, LinkName),
                    not_finished;
                false -> 
                    finished
            end
        end, not_finished, Links).
