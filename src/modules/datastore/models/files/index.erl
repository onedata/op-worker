%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing aggregated statistics about transfers
%%% featuring given space and target provider.
%%% @end
%%%-------------------------------------------------------------------
-module(index).
-author("Bartosz Walkowicz").
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    create/7, update/6, update/7,
    delete/2, list/1, list/4, save_db_view/6,
    query/3, get_json/2, is_supported/3, id/2, update_reduce_function/3, get/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type id() :: binary().
-type diff() :: datastore:diff(index()).
-type doc() :: datastore_doc:doc(index()).

-type name() :: binary().
-type key() :: datastore_utils:key().
-type index() :: #index{}.
-type index_function() :: binary().
-type options() :: proplists:proplist().
-type query_options() :: [couchbase_driver:view_opt()].
-type providers() :: [od_provider:id(), ...].

-export_type([name/0, index/0, index_function/0, doc/0, options/0, providers/0, query_options/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined()
}).
-define(DISK_CTX, (datastore_model_default:get_default_disk_ctx())).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new index (creates also design doc but only if this provider is on
%% specified providers list) and adds it to links tree.
%% @end
%%--------------------------------------------------------------------
-spec create(od_space:id(), name(), index_function(), undefined | index_function(),
    options(), boolean(), providers()) -> ok | {error, term()}.
create(SpaceId, Name, MapFunction, ReduceFunction, Options, Spatial, Providers) ->
    IndexId = id(Name, SpaceId),
    ToCreate = #document{
        key = IndexId,
        value = #index{
            name = Name,
            space_id = SpaceId,
            spatial = Spatial,
            map_function = index_utils:escape_js_function(MapFunction),
            reduce_function = ReduceFunction,
            index_options = Options,
            providers = Providers
        },
        scope = SpaceId
    },
    case create(ToCreate) of
        {ok, Doc} ->
            ok = index_links:add_link(Name, SpaceId),
            index_changes:handle(Doc),
            ok;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv update(SpaceId, Name, MapFunction, undefined, Options, Spatial, Providers).
%% @end
%%--------------------------------------------------------------------
-spec update(od_space:id(), name(), undefined | index_function(), options(),
    undefined | boolean(), undefined | providers()) -> ok | {error, term()}.
update(SpaceId, Name, MapFunction, Options, Spatial, Providers) ->
    update(SpaceId, Name, MapFunction, undefined, Options, Spatial, Providers).

%%--------------------------------------------------------------------
%% @doc
%% Updates definition for specified index in specified space.
%% By specifying given argument as 'undefined' old value will not be replaced.
%% @end
%%--------------------------------------------------------------------
-spec update(od_space:id(), name(), undefined | index_function(),
    undefined | index_function(), options(), undefined | boolean(),
    undefined | providers()) -> ok | {error, term()}.
update(SpaceId, Name, MapFunction, ReduceFunction, Options, Spatial, Providers) ->
    IndexId = id(Name, SpaceId),
    Diff = fun(OldIndex = #index{
        spatial = OldSpatial,
        map_function = OldMap,
        reduce_function = OldReduce,
        index_options = OldOptions,
        providers = OldProviders
    }) ->
        NewOptions = maps:to_list(maps:merge(
            maps:from_list(OldOptions), maps:from_list(Options)
        )),
        {ok, OldIndex#index{
            spatial = utils:ensure_defined(Spatial, undefined, OldSpatial),
            map_function = utils:ensure_defined(MapFunction, undefined, OldMap),
            reduce_function = utils:ensure_defined(ReduceFunction, undefined, OldReduce),
            index_options = NewOptions,
            providers = utils:ensure_defined(Providers, undefined, OldProviders)
        }}
    end,
    case update(IndexId, Diff) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates definition of reduce function for given index or deletes it in case
%% of specifying 'undefined' as value.
%% @end
%%--------------------------------------------------------------------
-spec update_reduce_function(od_space:id(), name(), undefined | index_function()) ->
    ok | {error, term()}.
update_reduce_function(SpaceId, Name, ReduceFunction) ->
    IndexId = id(Name, SpaceId),
    Diff = fun(Index) -> {ok, Index#index{reduce_function = ReduceFunction}} end,
    case update(IndexId, Diff) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

-spec is_supported(od_space:id(), binary(), od_provider:id()) -> boolean().
is_supported(SpaceId, IndexName, ProviderId) ->
    case datastore_model:get(?CTX, id(IndexName, SpaceId)) of
        {ok, #document{value = #index{providers = ProviderIds}}} ->
            lists:member(ProviderId, ProviderIds);
        _Error ->
            false
    end.

-spec get_json(od_space:id(), binary()) ->
    {ok, #{binary() => term()}} | {error, term()}.
get_json(SpaceId, IndexName) ->
    Id = id(IndexName, SpaceId),
    case datastore_model:get(?CTX, Id) of
        {ok, #document{value = #index{
            spatial = Spatial,
            map_function = MapFunction,
            reduce_function = ReduceFunction,
            index_options = Options,
            providers = Providers
        }}} ->
            {ok, #{
                <<"indexOptions">> => maps:from_list(Options),
                <<"providers">> => Providers,
                <<"mapFunction">> => MapFunction,
                <<"reduceFunction">> => utils:ensure_defined(
                    index_utils:escape_js_function(ReduceFunction), undefined, null
                ),
                <<"spatial">> => Spatial
            }};
        Error -> Error
    end.

-spec delete(od_space:id(), binary()) -> ok | {error, term()}.
delete(SpaceId, IndexName) ->
    Id = id(IndexName, SpaceId),
    datastore_model:delete(?CTX, Id),
    couchbase_driver:delete_design_doc(?DISK_CTX, Id),
    index_links:delete_links(IndexName, SpaceId).

-spec list(od_space:id()) -> {ok, [index:name()]}.
list(SpaceId) ->
    list(SpaceId, undefined, 0, all).

-spec list(od_space:id(), undefined | name(), integer(), non_neg_integer() | all) -> {ok, [index:name()]}.
list(SpaceId, StartId, Offset, Limit) ->
    {ok, index_links:list(SpaceId, StartId, Offset, Limit)}.

-spec get(od_space:id(), name()) -> {ok, doc()} | {error, term()}.
get(SpaceId, IndexName) ->
    datastore_model:get(?CTX, id(IndexName, SpaceId)).

-spec id(name(), od_space:id()) -> key().
id(IndexName, SpaceId) ->
    datastore_utils:gen_key(IndexName, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Add view to db, Function should be a valid, escaped javascript string
%% with one argument function.
%% @end
%%--------------------------------------------------------------------
-spec save_db_view(key(), od_space:id(), index_function(),
    undefined | index_function(), boolean(), options()) -> ok.
save_db_view(IndexId, SpaceId, Function, ReduceFunction, Spatial, Options) ->
    ViewFunction =
    <<"function (doc, meta) {
        'use strict';
        if(doc['_record'] == 'custom_metadata'
            && doc['space_id'] == '", SpaceId/binary, "'
            && doc['_deleted'] == false)
        {
            var user_map_callback = eval.call(null, '(", Function/binary, ")');
            var result = user_map_callback(doc['file_objectid'], doc['value']);
            if(result) {
                emit(result[0], result[1]);
            }
        }
        return null;
    }">>,
    ok = case Spatial of
        true ->
            couchbase_driver:save_spatial_view_doc(?DISK_CTX, IndexId, ViewFunction, Options);
        _ ->
            couchbase_driver:save_view_doc(?DISK_CTX, IndexId, ViewFunction, ReduceFunction, Options)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Query view.
%% @end
%%--------------------------------------------------------------------
-spec query(od_space:id(), name(), options()) -> {ok, datastore_json:ejson()} | {error, term()}.
query(SpaceId, <<"file-popularity">>, Options) ->
    Id = <<"file-popularity-", SpaceId/binary>>,
    couchbase_driver:query_view(?DISK_CTX, Id, Id, Options);
query(SpaceId, IndexName, Options) ->
    Id = id(IndexName, SpaceId),
    couchbase_driver:query_view(?DISK_CTX, Id, Id, Options).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec create(doc()) -> {ok, doc()} | {error, term()}.
create(Doc) ->
    datastore_model:save(?CTX, Doc).

%% @private
-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(IndexId, Diff) ->
    datastore_model:update(?CTX, IndexId, Diff).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {name, string},
        {space_id, string},
        {spatial, atom},
        {map_function, string},
        {reduce_function, string},
        {index_options, [{atom, term}]},
        {providers, [string]}
    ]}.
