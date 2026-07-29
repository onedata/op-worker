%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for testing views (db indexes) in onenv-based CT
%%% tests - generators of the view functions used as test data, creation
%%% and querying of views through the internal API, and cleanup.
%%%
%%% A view is a per-space couchbase design document evaluated by an
%%% explicitly given set of providers. Its doc is dbsync-synced to all
%%% supporting providers, but only the evaluating ones can be queried -
%%% hence the creation and awaiting helpers take provider selectors
%%% separately, and every await is generously budgeted: a freshly created
%%% view is indexed lazily and its first query has to wait out both the
%%% doc sync and the index build.
%%% @end
%%%-------------------------------------------------------------------
-module(view_test_utils).
-author("Bartosz Walkowicz").

-include("view_test.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([
    rand_view_name/1,

    gen_map_function/1, gen_map_function/2,
    gen_reversed_map_function/1,
    gen_spatial_map_function/1,
    gen_reduce_function/1,

    create_view/4,
    await_view_synced/3,
    await_query_result/5,
    remove_all_views/2
]).

-type view_spec() :: #{
    map_function := index:view_function(),
    reduce_function => undefined | index:view_function(),
    options => index:options(),
    spatial => boolean(),
    % providers evaluating the view; defaults to the one the view is saved on
    providers => [oct_background:entity_selector()]
}.

-export_type([view_spec/0]).

% for awaiting a view removal to be reflected in the listings of all the providers -
% budgeted for a dbsync round trip rather than an index build (see remove_all_views/2)
-define(VIEW_CLEANUP_ATTEMPTS, 30).


%%%===================================================================
%%% API
%%%===================================================================


-spec rand_view_name(atom()) -> index:name().
rand_view_name(CaseName) ->
    str_utils:format_bin("view_~ts_~ts", [CaseName, str_utils:rand_hex(6)]).


%%--------------------------------------------------------------------
%% @doc
%% Builds a view map function emitting, for every file having the given
%% xattr, its object id keyed by the xattr value.
%% @end
%%--------------------------------------------------------------------
-spec gen_map_function(onedata_file:xattr_name()) -> index:view_function().
gen_map_function(XattrName) ->
    <<"function (id, type, meta, ctx) {
        if (type == 'custom_metadata' && meta['", XattrName/binary, "']) {
            return [meta['", XattrName/binary, "'], id];
        }
        return null;
    }">>.


%%--------------------------------------------------------------------
%% @doc
%% Like gen_map_function/1 but emits [id, SecondXattrValue] pairs -
%% input for a reduce function filtering by the second xattr value
%% (see gen_reduce_function/1).
%% @end
%%--------------------------------------------------------------------
-spec gen_map_function(onedata_file:xattr_name(), onedata_file:xattr_name()) ->
    index:view_function().
gen_map_function(XattrName, SecondXattrName) ->
    <<"function (id, type, meta, ctx) {
        if (type == 'custom_metadata' && meta['", XattrName/binary, "']) {
            return [meta['", XattrName/binary, "'], [id, meta['", SecondXattrName/binary, "']]];
        }
        return null;
    }">>.


%%--------------------------------------------------------------------
%% @doc
%% Builds a view map function with the emissions of gen_map_function/1
%% reversed - the file object id becomes the key and the xattr value the
%% emitted value. Used to tell two views apart by their query results.
%% @end
%%--------------------------------------------------------------------
-spec gen_reversed_map_function(onedata_file:xattr_name()) -> index:view_function().
gen_reversed_map_function(XattrName) ->
    <<"function (id, type, meta, ctx) {
        if (type == 'custom_metadata' && meta['", XattrName/binary, "']) {
            return [id, meta['", XattrName/binary, "']];
        }
        return null;
    }">>.


%%--------------------------------------------------------------------
%% @doc
%% Builds a spatial view map function emitting, for every file having the
%% given key set in its json metadata, its object id keyed by that key's
%% value (expected to hold a GeoJSON geometry).
%% @end
%%--------------------------------------------------------------------
-spec gen_spatial_map_function(binary()) -> index:view_function().
gen_spatial_map_function(JsonMetadataKey) ->
    <<"function (id, type, meta, ctx) {
        if (type == 'custom_metadata' && meta['onedata_json'] && meta['onedata_json']['",
            JsonMetadataKey/binary, "']) {
            return [meta['onedata_json']['", JsonMetadataKey/binary, "'], id];
        }
        return null;
    }">>.


%%--------------------------------------------------------------------
%% @doc
%% Builds a view reduce function passing through only the file ids of the
%% [id, XattrValue] pairs (emitted by gen_map_function/2) with the given
%% xattr value.
%% @end
%%--------------------------------------------------------------------
-spec gen_reduce_function(term()) -> index:view_function().
gen_reduce_function(XattrValue) ->
    XattrValueBin = str_utils:to_binary(XattrValue),
    <<"function (key, values, rereduce) {
        var filtered = [];
        for (i = 0; i < values.length; i++)
            if (values[i][1] == ", XattrValueBin/binary, ")
                filtered.push(values[i][0]);
        return filtered;
    }">>.


%%--------------------------------------------------------------------
%% @doc
%% Saves a view doc on the given provider. The view is evaluated by the
%% providers declared in the spec (by default only the one it is saved
%% on); the other supporting providers merely receive the doc via dbsync
%% and must await it explicitly (see await_view_synced/3).
%% @end
%%--------------------------------------------------------------------
-spec create_view(
    oct_background:entity_selector(),
    od_space:id(),
    index:name(),
    view_spec()
) ->
    ok.
create_view(SavingProviderSelector, SpaceId, ViewName, ViewSpec) ->
    EvaluatingProviderIds = lists:map(
        fun oct_background:get_provider_id/1,
        maps:get(providers, ViewSpec, [SavingProviderSelector])
    ),
    ok = opw_test_rpc:call(SavingProviderSelector, index, save, [
        SpaceId, ViewName,
        maps:get(map_function, ViewSpec),
        maps:get(reduce_function, ViewSpec, undefined),
        maps:get(options, ViewSpec, []),
        maps:get(spatial, ViewSpec, false),
        EvaluatingProviderIds
    ]).


-spec await_view_synced(oct_background:entity_selector(), od_space:id(), index:name()) ->
    ok.
await_view_synced(ProviderSelector, SpaceId, ViewName) ->
    ?assertMatch({ok, _}, opw_test_rpc:call(
        ProviderSelector, index, get, [ViewName, SpaceId]
    ), ?VIEW_SYNC_ATTEMPTS),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Awaits the view emitting exactly the expected values (in any order)
%% for the given query on every one of the given providers. A query
%% failure (the view not synced yet or its index not built) is retried
%% rather than propagated, so that this can be called right after
%% create_view/4.
%% @end
%%--------------------------------------------------------------------
-spec await_query_result(
    [oct_background:entity_selector()],
    od_space:id(),
    index:name(),
    index:query_options(),
    [term()]
) ->
    ok.
await_query_result(ProviderSelectors, SpaceId, ViewName, QueryOptions, ExpectedValues) ->
    lists_utils:pforeach(fun(ProviderSelector) ->
        ?assertEqual(lists:sort(ExpectedValues), try
            {ok, #{<<"rows">> := Rows}} = opw_test_rpc:call(ProviderSelector, index, query, [
                SpaceId, ViewName, QueryOptions
            ]),
            lists:sort(lists:flatmap(fun(Row) ->
                lists:flatten([maps:get(<<"value">>, Row)])
            end, Rows))
        catch _:_ ->
            query_failed
        end, ?VIEW_SYNC_ATTEMPTS)
    end, ProviderSelectors).


%%--------------------------------------------------------------------
%% @doc
%% Removes all views of the space on every given provider. Called as part
%% of the leftover cleanup - view test cases create views with per-run
%% random names, which would otherwise accumulate.
%% @end
%%--------------------------------------------------------------------
-spec remove_all_views([oct_background:entity_selector()], od_space:id()) -> ok.
remove_all_views(ProviderSelectors, SpaceId) ->
    % A single pass does not suffice: a view deleted on one provider vanishes from
    % the others' listings only once dbsync delivers the deletion, and deleting a
    % view that its owner is deleting at the same time can leave the entry behind
    % altogether - with the view doc already gone, the deletion falls back to
    % removing the link by the given name, while a foreign view is listed under a
    % name disambiguated with the owner's id, which matches no link. Both cases
    % resolve within a few passes - by a repeated delete, once the name is no longer
    % ambiguous, or by dbsync catching up - so keep deleting until all the listings
    % are empty.
    ?assertEqual([], begin
        lists_utils:pforeach(fun(ProviderSelector) ->
            remove_listed_views(ProviderSelector, SpaceId)
        end, ProviderSelectors),
        lists:usort(lists:flatmap(fun(ProviderSelector) ->
            list_views(ProviderSelector, SpaceId)
        end, ProviderSelectors))
    end, ?VIEW_CLEANUP_ATTEMPTS),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec remove_listed_views(oct_background:entity_selector(), od_space:id()) -> ok.
remove_listed_views(ProviderSelector, SpaceId) ->
    lists:foreach(fun(ViewName) ->
        case opw_test_rpc:call(ProviderSelector, index, delete, [SpaceId, ViewName]) of
            ok ->
                ok;
            {error, not_found} ->
                % already deleted alongside the other provider (dbsync)
                ok
        end
    end, list_views(ProviderSelector, SpaceId)).


%% @private
-spec list_views(oct_background:entity_selector(), od_space:id()) -> [index:name()].
list_views(ProviderSelector, SpaceId) ->
    {ok, ViewNames} = opw_test_rpc:call(ProviderSelector, index, list, [SpaceId]),
    ViewNames.
