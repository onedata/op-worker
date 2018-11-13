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
    save/7, update/6, update/7, get/1, get/2,
    delete/2, list/1, list/4, save_db_view/6, delete_db_view/2,
    query/3, get_json/2, is_supported/3, id/2, update_reduce_function/3
]).

%% datastore_model callbacks
-export([
    get_ctx/0, get_record_struct/1, get_record_version/0, get_posthooks/0
]).

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
-spec save(od_space:id(), name(), index_function(), undefined | index_function(),
    options(), boolean(), providers()) -> ok | {error, term()}.
save(SpaceId, Name, MapFunction, ReduceFunction, Options, Spatial, Providers) ->
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
    case save(ToCreate) of
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
            map_function = utils:ensure_defined(
                index_utils:escape_js_function(MapFunction), undefined, OldMap
            ),
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
    ?MODULE:get(id(IndexName, SpaceId)).

-spec get(id()) -> {ok, doc()} | {error, term()}.
get(IndexId) ->
    datastore_model:get(?CTX, IndexId).

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
    ViewFunction = map_function_wrapper(Function, SpaceId),
    ok = case Spatial of
        true ->
            couchbase_driver:save_spatial_view_doc(?DISK_CTX, IndexId, ViewFunction, Options);
        _ ->
            couchbase_driver:save_view_doc(?DISK_CTX, IndexId, ViewFunction, ReduceFunction, Options)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes view from db.
%% @end
%%--------------------------------------------------------------------
-spec delete_db_view(od_space:id(), binary()) -> ok | {error, term()}.
delete_db_view(SpaceId, IndexName) ->
    Id = id(IndexName, SpaceId),
    couchbase_driver:delete_design_doc(?DISK_CTX, Id).

%%--------------------------------------------------------------------
%% @doc
%% Query view.
%% @end
%%--------------------------------------------------------------------
-spec query(od_space:id(), name(), options()) ->
    {ok, datastore_json:ejson()} | {error, term()}.
query(SpaceId, <<"file-popularity">>, Options) ->
    query(<<"file-popularity-", SpaceId/binary>>, Options);
query(SpaceId, IndexName, Options) ->
    query(id(IndexName, SpaceId), Options).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec save(doc()) -> {ok, doc()} | {error, term()}.
save(Doc) ->
    datastore_model:save(?CTX, Doc).

%% @private
-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(IndexId, Diff) ->
    datastore_model:update(?CTX, IndexId, Diff).

%% @private
-spec query(id(), options()) -> {ok, datastore_json:ejson()} | {error, term()}.
query(IndexId, Options) ->
    case couchbase_driver:query_view(?DISK_CTX, IndexId, IndexId, Options) of
        {ok, _} = Ans ->
            Ans;
        {error, {<<"case_clause">>, <<"{not_found,deleted}">>}} ->
            {error, not_found};
        Error ->
            Error
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Posthook responsible for calling index_changes:handle function
%% for locally updated document.
%% @end
%%-------------------------------------------------------------------
-spec run_on_index_doc_change(atom(), list(), term()) -> {ok, doc()}.
run_on_index_doc_change(update, [_, _, _], Result = {ok, Doc}) ->
    index_changes:handle(Doc),
    Result;
run_on_index_doc_change(_, _, Result) ->
    Result.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% JS wrapper for user-defined mapping function.
%% It enables creation of Couchbase views on the following models:
%%     * file_meta,
%%     * times,
%%     * custom_metadata,
%%     * file_popularity.
%%
%% More info on Couchbase views and writing mapping functions:
%%     https://docs.couchbase.com/server/5.5/views/views-writing-map.html
%%
%% All fields starting with underscore in the above models are filtered out
%% and not passed to the mapping function.
%%
%% The mapping function should accept 4 arguments:
%%     * id - cdmi object id of a file,
%%     * type - type of the document that is being mapped by the function. One of:
%%         ** "file_meta"
%%         ** "times"
%%         ** "custom_metadata"
%%         ** "file_popularity"
%%     * meta - values stored in the document being mapped,
%%     * ctx - context object used for storing helpful information. Currently it stores:
%%         ** provider_id,
%%         ** space_id.
%%
%% The mapping function should return (key, value) pair/s that are to be emitted
%% to the view via emit(...) function.
%%
%% If one document shall be mapped to exactly one row in the view, the mapping
%% function should return 2-element list [key, value],
%% where key and value can be any JS object.
%%
%% If one document shall be mapped to many rows in the view, the mapping
%% function should return an object with the key "list". The value should be
%% a list of 2-element lists [key, value].
%%
%% Examples of the mapping function:
%%
%%    * returning a single view row
%%
%%      function (id, type, meta, ctx) {
%%          var key = ...
%%          var value = ...
%%          return [key, value];
%%      }
%%
%%    * returning multiple view rows
%%
%%      function (id, type, meta, ctx) {
%%          var key1 = ...
%%          var value1 = ...
%%          var key2 = ...
%%          var value2 = ...
%%          .
%%          .
%%          .
%%          var keyN = ...
%%          var valueN = ...
%%
%%          return {"list": [
%%              [key1, value1],
%%              [key2, value2],
%%              .
%%              .
%%              .
%%              [keyN, valueN],
%%          ]};
%%      }
%% @end
%%-------------------------------------------------------------------
-spec map_function_wrapper(binary(), od_space:id()) -> binary().
map_function_wrapper(UserMapFunction, SpaceId) -> <<
    "function (doc, meta) {
        'use strict';
        var userMapCallback = eval.call(null, '(", UserMapFunction/binary, ")');

        // code for building cdmi_id
        ", (build_cdmi_object_id_in_js())/binary,"

        function filterHiddenValues(object) {
            var filtered = {}
            for (var key of Object.keys(object))
                if (!key.startsWith('_'))
                    filtered[key] = object[key];
            return filtered;
        };

        var spaceId = doc['_scope'];

        if(spaceId == '", SpaceId/binary, "' && doc['_deleted'] == false) {

            var id = null;
            var type = doc['_record'];
            var meta = null;
            var ctx = {
                'spaceId': spaceId,
                'providerId': '", (oneprovider:get_id())/binary ,"'
            };

            switch (type) {
                case 'file_meta':
                    id = buildObjectId(doc['_key'], spaceId);
                    meta = filterHiddenValues(doc);
                    delete meta['is_scope'];
                    delete meta['scope'];
                    break;
                case 'times':
                    id = buildObjectId(doc['_key'], spaceId);
                    meta = filterHiddenValues(doc);
                    break;
                case 'custom_metadata':
                    id = doc['file_objectid'];
                    meta = doc['value'];
                    break;
                case 'file_popularity':
                    id = buildObjectId(doc['file_uuid'], spaceId);
                    meta = filterHiddenValues(doc);
                    break;
                default:
                    return null;
            }

            var result = userMapCallback(id, type, meta, ctx);

            if(result) {
                if ('list' in result) {
                    for (var keyValuePair of result['list'])
                        emit(keyValuePair[0], keyValuePair[1]);
                }
                else{
                    emit(result[0], result[1]);
                }
            }
            return null;
        }
        return null;
    }">>.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Set of JS functions used for converting uuid to cdmi object id.
%% https://github.com/kliput/onedata-uuid-to-objectid
%% @end
%%-------------------------------------------------------------------
-spec build_cdmi_object_id_in_js() -> binary().
build_cdmi_object_id_in_js() -> <<
    "function crc16(arr) {
        var table = [
            0x0000, 0xC0C1, 0xC181, 0x0140, 0xC301, 0x03C0, 0x0280, 0xC241,
            0xC601, 0x06C0, 0x0780, 0xC741, 0x0500, 0xC5C1, 0xC481, 0x0440,
            0xCC01, 0x0CC0, 0x0D80, 0xCD41, 0x0F00, 0xCFC1, 0xCE81, 0x0E40,
            0x0A00, 0xCAC1, 0xCB81, 0x0B40, 0xC901, 0x09C0, 0x0880, 0xC841,
            0xD801, 0x18C0, 0x1980, 0xD941, 0x1B00, 0xDBC1, 0xDA81, 0x1A40,
            0x1E00, 0xDEC1, 0xDF81, 0x1F40, 0xDD01, 0x1DC0, 0x1C80, 0xDC41,
            0x1400, 0xD4C1, 0xD581, 0x1540, 0xD701, 0x17C0, 0x1680, 0xD641,
            0xD201, 0x12C0, 0x1380, 0xD341, 0x1100, 0xD1C1, 0xD081, 0x1040,
            0xF001, 0x30C0, 0x3180, 0xF141, 0x3300, 0xF3C1, 0xF281, 0x3240,
            0x3600, 0xF6C1, 0xF781, 0x3740, 0xF501, 0x35C0, 0x3480, 0xF441,
            0x3C00, 0xFCC1, 0xFD81, 0x3D40, 0xFF01, 0x3FC0, 0x3E80, 0xFE41,
            0xFA01, 0x3AC0, 0x3B80, 0xFB41, 0x3900, 0xF9C1, 0xF881, 0x3840,
            0x2800, 0xE8C1, 0xE981, 0x2940, 0xEB01, 0x2BC0, 0x2A80, 0xEA41,
            0xEE01, 0x2EC0, 0x2F80, 0xEF41, 0x2D00, 0xEDC1, 0xEC81, 0x2C40,
            0xE401, 0x24C0, 0x2580, 0xE541, 0x2700, 0xE7C1, 0xE681, 0x2640,
            0x2200, 0xE2C1, 0xE381, 0x2340, 0xE101, 0x21C0, 0x2080, 0xE041,
            0xA001, 0x60C0, 0x6180, 0xA141, 0x6300, 0xA3C1, 0xA281, 0x6240,
            0x6600, 0xA6C1, 0xA781, 0x6740, 0xA501, 0x65C0, 0x6480, 0xA441,
            0x6C00, 0xACC1, 0xAD81, 0x6D40, 0xAF01, 0x6FC0, 0x6E80, 0xAE41,
            0xAA01, 0x6AC0, 0x6B80, 0xAB41, 0x6900, 0xA9C1, 0xA881, 0x6840,
            0x7800, 0xB8C1, 0xB981, 0x7940, 0xBB01, 0x7BC0, 0x7A80, 0xBA41,
            0xBE01, 0x7EC0, 0x7F80, 0xBF41, 0x7D00, 0xBDC1, 0xBC81, 0x7C40,
            0xB401, 0x74C0, 0x7580, 0xB541, 0x7700, 0xB7C1, 0xB681, 0x7640,
            0x7200, 0xB2C1, 0xB381, 0x7340, 0xB101, 0x71C0, 0x7080, 0xB041,
            0x5000, 0x90C1, 0x9181, 0x5140, 0x9301, 0x53C0, 0x5280, 0x9241,
            0x9601, 0x56C0, 0x5780, 0x9741, 0x5500, 0x95C1, 0x9481, 0x5440,
            0x9C01, 0x5CC0, 0x5D80, 0x9D41, 0x5F00, 0x9FC1, 0x9E81, 0x5E40,
            0x5A00, 0x9AC1, 0x9B81, 0x5B40, 0x9901, 0x59C0, 0x5880, 0x9841,
            0x8801, 0x48C0, 0x4980, 0x8941, 0x4B00, 0x8BC1, 0x8A81, 0x4A40,
            0x4E00, 0x8EC1, 0x8F81, 0x4F40, 0x8D01, 0x4DC0, 0x4C80, 0x8C41,
            0x4400, 0x84C1, 0x8581, 0x4540, 0x8701, 0x47C0, 0x4680, 0x8641,
            0x8201, 0x42C0, 0x4380, 0x8341, 0x4100, 0x81C1, 0x8081, 0x4040
        ];
        var crc = 0x0000;
        for(var i = 0; i < arr.length; i++) {
            var byte = arr[i];
            crc = crc >>> 8 ^ table[ ( crc ^ byte ) & 0xff ];
        }
        return [(crc & 0xff00) >> 8, crc & 0x00ff];
    }

    function getGuid(uuid, spaceId) {
        return 'guid#' + uuid + '#' + spaceId;
    }

    function buildObjectIdFromGuid(data) {
        var _length = data.length;
        if (_length <= 320) {
            var bin = [0, 0, 0, 0, 0, _length, 0, 0];
            for (var i = 0; i < _length; i++) {
                bin.push(data.charCodeAt(i) || 0);
            }
            var crc = crc16(bin);
            var bin2 = [0, 0, 0, 0, 0, _length, crc[0], crc[1]];
            for (let i = 0; i < _length; i++) {
                bin2.push(data.charCodeAt(i) || 0);
            }
            return bin2.map(function(i) { return (i < 16 ? '0' : '') + i.toString(16) }).join('').toUpperCase();
        } else {
            throw new Error('buildObjectId: data too large ' + data);
        }
    };

    function buildObjectId(uuid, spaceId) {
        return buildObjectIdFromGuid(getGuid(uuid, spaceId));
    }
    ">>.

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
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [
        fun run_on_index_doc_change/3
    ].


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
