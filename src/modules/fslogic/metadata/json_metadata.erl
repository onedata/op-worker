%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API for files' json metadata. This api differs from one offered for
%%% other xattrs in following aspects:
%%% - 'inherited' flag causes all ancestors json metadata to be gathered
%%% and merged before returning rather than getting the first ancestor
%%% metadata with defined xattr undef specified key (as it happens for
%%% other xattrs)
%%% - Filters, that is path under/from which json metadata should be
%%% set/fetched.
%%% @end
%%%-------------------------------------------------------------------
-module(json_metadata).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([get/4, set/6, remove/2]).


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
                    {ok, json_utils:merge(GatheredJsons)}
            end;
        false ->
            get_direct_json_metadata(UserCtx, FileCtx)
    end,
    case Result of
        {ok, Json} ->
            case json_utils:query(Json, Filter) of
                {ok, _} = Ans -> Ans;
                error -> ?ERROR_NOT_FOUND
            end;
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
gather_ancestors_json_metadata(UserCtx, FileCtx0, GatheredMetadata) ->
    AllMetadata = case get_direct_json_metadata(UserCtx, FileCtx0) of
        {ok, Metadata} ->
            [Metadata | GatheredMetadata];
        ?ERROR_NOT_FOUND ->
            GatheredMetadata
    end,

    case file_ctx:get_and_check_parent(FileCtx0, UserCtx) of
        {undefined, _FileCtx1} ->
            {ok, AllMetadata};
        {ParentCtx, _FileCtx1} ->
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
                ?JSON_METADATA_KEY => case json_utils:insert(undefined, JsonToInsert, Names) of
                    {ok, Json} -> Json;
                    error -> throw({error, ?ENOATTR})
                end
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
                PrevJson = maps:get(?JSON_METADATA_KEY, MetaValue, undefined),
                case json_utils:insert(PrevJson, JsonToInsert, Names) of
                    {ok, NewJson} ->
                        {ok, Meta#custom_metadata{
                            value = MetaValue#{?JSON_METADATA_KEY => NewJson}
                        }};
                    error ->
                        {error, ?ENOATTR}
                end
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
