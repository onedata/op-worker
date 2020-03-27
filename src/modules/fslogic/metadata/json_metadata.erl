%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API for files' json metadata.
%%% @end
%%%-------------------------------------------------------------------
-module(json_metadata).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([get/4, set/6, remove/2]).

%% Private API - Export for unit and ct testing
-export([find/2, insert/3, merge/1]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get(
    user_ctx:ctx(),
    file_ctx:ctx(),
    custom_metadata:filter(),
    Inherited :: boolean()
) ->
    {ok, custom_metadata:value()} | {error, term()}.
get(UserCtx, FileCtx, Filter, Inherited) ->
    Result = case Inherited of
        true ->
            case gather_ancestors_json_metadata(UserCtx, FileCtx, []) of
                {ok, []} ->
                    ?ERROR_NOT_FOUND;
                {ok, GatheredJsons} ->
                    {ok, merge(GatheredJsons)}
            end;
        false ->
            get_direct_json_metadata(UserCtx, FileCtx)
    end,
    case Result of
        {ok, Json} ->
            {ok, find(Json, Filter)};
        {error, _} = Error ->
            Error
    end.


-spec set(
    user_ctx:ctx(),
    file_ctx:ctx(),
    json_utils:json_term(),
    custom_metadata:filter(),
    Create :: boolean(),
    Replace :: boolean()
) ->
    {ok, file_meta:uuid()} | {error, term()}.
set(UserCtx, FileCtx0, Json, Names, Create, Replace) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_metadata]
    ),
    set_insecure(FileCtx1, Json, Names, Create, Replace).


-spec remove(user_ctx:ctx(), file_ctx:ctx()) -> ok | {error, term()}.
remove(UserCtx, FileCtx) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx,
        [traverse_ancestors, ?write_metadata]
    ),
    FileUuid = file_ctx:get_uuid_const(FileCtx1),
    custom_metadata:remove_xattr(FileUuid, ?JSON_METADATA_KEY).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec gather_ancestors_json_metadata(
    user_ctx:ctx(),
    file_ctx:ctx(),
    [custom_metadata:value()]
) ->
    {ok, custom_metadata:value()} | {error, term()}.
gather_ancestors_json_metadata(UserCtx, FileCtx, GatheredMetadata) ->
    AllMetadata = case get_direct_json_metadata(UserCtx, FileCtx) of
        {ok, Metadata} ->
            [Metadata | GatheredMetadata];
        ?ERROR_NOT_FOUND ->
            GatheredMetadata
    end,

    FileGuid = file_ctx:get_guid_const(FileCtx),
    {ParentCtx, _FileCtx1} = file_ctx:get_parent(FileCtx, UserCtx),

    case file_ctx:get_guid_const(ParentCtx) of
        FileGuid ->
            % root dir/share root file -> there are no parents
            {ok, AllMetadata};
        _ ->
            gather_ancestors_json_metadata(UserCtx, ParentCtx, AllMetadata)
    end.


%% @private
-spec get_direct_json_metadata(user_ctx:ctx(), file_ctx:ctx()) ->
    {ok, custom_metadata:value()} | {error, term()}.
get_direct_json_metadata(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_metadata]
    ),
    FileUuid = file_ctx:get_uuid_const(FileCtx1),
    custom_metadata:get_xattr(FileUuid, ?JSON_METADATA_KEY).


%% @private
-spec set_insecure(
    file_ctx:ctx(),
    json_utils:json_term(),
    custom_metadata:filter(),
    Create :: boolean(),
    Replace :: boolean()
) ->
    {ok, file_meta:uuid()} | {error, term()}.
set_insecure(FileCtx, JsonToInsert, Names, Create, Replace) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {ok, FileObjectId} = file_id:guid_to_objectid(file_ctx:get_guid_const(FileCtx)),
    ToCreate = #document{
        key = FileUuid,
        value = #custom_metadata{
            space_id = file_ctx:get_space_id_const(FileCtx),
            file_objectid = FileObjectId,
            value = #{
                ?JSON_METADATA_KEY => insert(undefined, JsonToInsert, Names)
            }
        },
        scope = file_ctx:get_space_id_const(FileCtx)
    },
    Diff = fun(Meta = #custom_metadata{value = MetaValue}) ->
        case {maps:is_key(?JSON_METADATA_KEY, MetaValue), Create, Replace} of
            {true, true, _} ->
                {error, ?EEXIST};
            {false, _, true} ->
                {error, ?ENODATA};
            _ ->
                Json = maps:get(?JSON_METADATA_KEY, MetaValue, #{}),
                NewJson = insert(Json, JsonToInsert, Names),
                {ok, Meta#custom_metadata{value = MetaValue#{?JSON_METADATA_KEY => NewJson}}}
        end
    end,
    case Replace of
        true ->
            case custom_metadata:update(FileUuid, Diff) of
                {error, not_found} ->
                    {error, ?ENODATA};
                OtherAns ->
                    OtherAns
            end;
        false ->
            custom_metadata:create_or_update(ToCreate, Diff)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Find sub-json in json tree
%% @end
%%--------------------------------------------------------------------
-spec find(json_utils:json_term(), custom_metadata:filter()) ->
    json_utils:json_term() | no_return().
find(Json, []) ->
    Json;
find(Json, [Name | Rest]) ->
    IndexSize = get_index_size(Name),
    case Name of
        <<"[", Element:IndexSize/binary, "]">> when is_list(Json) ->
            Index = binary_to_integer(Element) + 1,
            case length(Json) < Index of
                true ->
                    throw({error, ?ENOATTR});
                false ->
                    SubJson = lists:nth(Index, Json),
                    find(SubJson, Rest)
            end;
        Name when is_map(Json) ->
            case maps:find(Name, Json) of
                error ->
                    throw({error, ?ENOATTR});
                {ok, SubJson} ->
                    find(SubJson, Rest)
            end;
        _ ->
            throw({error, ?ENOATTR})
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Insert sub-json to json tree
%% @end
%%--------------------------------------------------------------------
-spec insert(Json :: map() | undefined, JsonToInsert :: map(), [binary()]) -> map() | no_return().
insert(_Json, JsonToInsert, []) ->
    JsonToInsert;
insert(undefined, JsonToInsert, [Name | Rest]) ->
    IndexSize = get_index_size(Name),
    case Name of
        <<"[", Element:IndexSize/binary, "]">> ->
            Index = binary_to_integer(Element) + 1,
            [null || _ <- lists:seq(1, Index - 1)] ++ [insert(undefined, JsonToInsert, Rest)];
        _ ->
            maps:put(Name, insert(undefined, JsonToInsert, Rest), #{})
    end;
insert(Json, JsonToInsert, [Name | Rest]) ->
    IndexSize = get_index_size(Name),
    case Name of
        <<"[", Element:IndexSize/binary, "]">> when is_list(Json) ->
            Index = binary_to_integer(Element) + 1,
            Length = length(Json),
            case Length < Index of
                true ->
                    Json ++ [null || _ <- lists:seq(Length + 1, Index - 1)] ++ [insert(undefined, JsonToInsert, Rest)];
                false ->
                    setnth(Index, Json, insert(undefined, JsonToInsert, Rest))
            end;
        _ when is_map(Json) ->
            SubJson = maps:get(Name, Json, undefined),
            maps:put(Name, insert(SubJson, JsonToInsert, Rest), Json);
        _ ->
            throw({error, ?ENOATTR})
    end.


%% @private
-spec merge([json_utils:json_term()]) -> json_utils:json_term().
merge(Jsons) ->
    lists:foldl(fun
        (Json, ParentJson) when is_map(Json) andalso is_map(ParentJson) ->
            ChildKeys = maps:keys(Json),
            ParentKeys = maps:keys(ParentJson),
            ChildOnlyKey = ChildKeys -- ParentKeys,
            CommonKeys = ChildKeys -- ChildOnlyKey,

            ResultingJson = maps:merge(
                ParentJson,
                maps:with(ChildOnlyKey, Json)
            ),

            lists:foldl(fun(Key, Acc) ->
                ChildValue = maps:get(Key, Json),
                ParentValue = maps:get(Key, ParentJson),
                Acc#{Key => merge([ParentValue, ChildValue])}
            end, ResultingJson, CommonKeys);

        (Json, _ParentJson) ->
            Json
    end, #{}, Jsons).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get byte size of json array index stored in binary: e. g. "[12]" -> 2
%% @end
%%--------------------------------------------------------------------
-spec get_index_size(Name :: binary()) -> integer().
get_index_size(Name) ->
    byte_size(Name) - 2.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Set nth element of list
%% @end
%%--------------------------------------------------------------------
-spec setnth(non_neg_integer(), list(), term()) -> list().
setnth(1, [_ | Rest], New) -> [New | Rest];
setnth(I, [E | Rest], New) -> [E | setnth(I - 1, Rest, New)].
