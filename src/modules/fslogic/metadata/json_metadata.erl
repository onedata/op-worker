%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc API for files' json metadata.
%%% @end
%%%-------------------------------------------------------------------
-module(json_metadata).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/metadata.hrl").

%% API
-export([get/3, set/3, remove/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Gets json metadata subtree
%% e. g. for meta:
%%
%% {'l1': {'l2': 'value'}}
%%
%% get_json_metadata(FileUuid, [<<"l1">>, <<"l2">>]) -> {ok, <<"value">>}
%% get_json_metadata(FileUuid, [<<"l1">>]) -> {ok, #{<<"l2">> => <<"value">>}}
%% get_json_metadata(FileUuid, []) -> {ok, #{<<"l1">> => {<<"l2">> => <<"value">>}}}
%%
%% @end
%%--------------------------------------------------------------------
-spec get(file_ctx:ctx(), custom_metadata:filter(), Inherited :: boolean()) ->
    {ok, custom_metadata:json()} | {error, term()}.
get(FileCtx, Names, false) ->
    case custom_metadata:get(file_ctx:get_uuid_const(FileCtx)) of
        {ok, #document{value = #custom_metadata{value = #{?JSON_METADATA_KEY := Json}}}} ->
            {ok, custom_meta_manipulation:find(Json, Names)};
        {ok, #document{value = #custom_metadata{}}} ->
            {error, {not_found,custom_metadata}};
        Error ->
            Error
    end;
get(FileCtx, Names, true) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    case file_meta:get_ancestors(FileUuid) of
        {ok, Uuids} ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            Jsons = lists:map(fun(Uuid) ->
                AncestorCtx = file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(Uuid, SpaceId)),
                case get(AncestorCtx, Names, false) of
                    {ok, Json} ->
                        Json;
                    {error, {not_found,custom_metadata}} ->
                        #{}
                end
            end, [FileUuid | Uuids]),
            {ok, custom_meta_manipulation:merge(Jsons)};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc Set json metadata subtree
%% e. g. for meta:
%%
%% {'l1': {'l2': 'value'}}
%%
%% set_json_metadata(FileUuid, <<"new_value">> [<<"l1">>, <<"l2">>])
%%    meta: {'l1': {'l2': 'new_value'}}
%% set_json_metadata(FileUuid, [<<"l1">>])
%%    meta: {'l1': 'new_value'}
%% set_json_metadata(FileUuid, []) -> {ok, #{<<"l1">> => {<<"l2">> => <<"value">>}}}
%%    meta: 'new_value'
%%--------------------------------------------------------------------
-spec set(file_ctx:ctx(), custom_metadata:json(), [binary()]) ->
    {ok, file_meta:uuid()} | {error, term()}.
set(FileCtx, JsonToInsert, Names) ->
    ToCreate = #document{key = file_ctx:get_uuid_const(FileCtx), value = #custom_metadata{
        space_id = file_ctx:get_space_id_const(FileCtx),
        value = #{?JSON_METADATA_KEY => custom_meta_manipulation:insert(undefined, JsonToInsert, Names)}
    }},
    custom_metadata:create_or_update(ToCreate, fun(Meta = #custom_metadata{value = MetaValue}) ->
        Json = maps:get(?JSON_METADATA_KEY, MetaValue, #{}),
        NewJson = custom_meta_manipulation:insert(Json, JsonToInsert, Names),
        {ok, Meta#custom_metadata{value = MetaValue#{?JSON_METADATA_KEY => NewJson}}}
    end).

%%--------------------------------------------------------------------
%% @doc Removes file's json metadata
%% @equiv remove_xattr_metadata(FileUuid, ?JSON_METADATA_KEY).
%%--------------------------------------------------------------------
-spec remove(file_ctx:ctx()) -> ok | {error, term()}.
remove(FileCtx) ->
    custom_metadata:remove_xattr_metadata(file_ctx:get_uuid_const(FileCtx), ?JSON_METADATA_KEY).