%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for holding files' custom metadata.
%%% @end
%%%-------------------------------------------------------------------
-module(custom_metadata).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([get/1, update/2, delete/1, create_or_update/2]).
-export([get_xattr_metadata/3, list_xattr_metadata/2, exists_xattr_metadata/2,
    remove_xattr_metadata/2, set_xattr_metadata/6]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

% Metadata types
-type type() :: json | rdf.
-type name() :: binary().
-type value() :: rdf() | jiffy:json_value().
-type names() :: [name()].
-type doc() :: datastore_doc:doc(metadata()).
-type diff() :: datastore_doc:diff(metadata()).
-type metadata() :: #metadata{}.
-type rdf() :: binary().
-type view_id() :: binary().
-type filter() :: [binary()].

-export_type([type/0, name/0, value/0, names/0, metadata/0, rdf/0, view_id/0,
    filter/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns custom metadata.
%% @end
%%--------------------------------------------------------------------
-spec get(file_meta:uuid()) -> {ok, doc()} | {error, term()}.
get(FileUuid) ->
    datastore_model:get(?CTX, FileUuid).

%%--------------------------------------------------------------------
%% @doc
%% Updates custom metadata.
%% @end
%%--------------------------------------------------------------------
-spec update(file_meta:uuid(), diff()) ->
    {ok, file_meta:uuid()} | {error, term()}.
update(FileUuid, Diff) ->
    ?extract_key(datastore_model:update(?CTX, FileUuid, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Deletes custom metadata.
%% @end
%%--------------------------------------------------------------------
-spec delete(file_meta:uuid()) -> ok | {error, term()}.
delete(FileUuid) ->
    datastore_model:delete(?CTX, FileUuid).

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(doc(), diff()) ->
    {ok, file_meta:uuid()} | {error, term()}.
create_or_update(#document{key = Key, value = Default, scope = Scope}, Diff) ->
    ?extract_key(datastore_model:update(
        ?CTX#{scope => Scope}, Key, Diff, Default)
    ).

%%--------------------------------------------------------------------
%% @doc
%% Get extended attribute metadata.
%% @end
%%--------------------------------------------------------------------
-spec get_xattr_metadata(file_meta:uuid(), xattr:name(), Inherited :: boolean()) ->
    {ok, xattr:value()} | {error, term()}.
get_xattr_metadata(?ROOT_DIR_UUID, Name, true) ->
    get_xattr_metadata(?ROOT_DIR_UUID, Name, false);
get_xattr_metadata(FileUuid, Name, true) ->
    case get_xattr_metadata(FileUuid, Name, false) of
        {ok, Value} ->
            {ok, Value};
        {error, not_found} ->
            case file_meta:get_parent_uuid({uuid, FileUuid}) of
                {ok, ParentUuid} ->
                    get_xattr_metadata(ParentUuid, Name, true);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end;
get_xattr_metadata(FileUuid, Name, false) ->
    case datastore_model:get(?CTX, FileUuid) of
        {ok, #document{value = #custom_metadata{value = Meta}}} ->
            case maps:find(Name, Meta) of
                {ok, Value} -> {ok, Value};
                error -> {error, not_found}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% List extended attribute metadata names.
%% @end
%%--------------------------------------------------------------------
-spec list_xattr_metadata(file_meta:uuid(), Inherited :: boolean()) ->
    {ok, [xattr:name()]} | {error, term()}.
list_xattr_metadata(FileUuid, true) ->
    case file_meta:get_ancestors(FileUuid) of
        {ok, Uuids} ->
            Xattrs = lists:foldl(fun(Uuid, Acc) ->
                case list_xattr_metadata(Uuid, false) of
                    {ok, Json} ->
                        Acc ++ Json;
                    {error, not_found} ->
                        Acc
                end
            end, [], [FileUuid | Uuids]),
            UniqueAttrs = lists:usort(Xattrs),
            {ok, UniqueAttrs};
        {error, Reason} ->
            {error, Reason}
    end;
list_xattr_metadata(FileUuid, false) ->
    case datastore_model:get(?CTX, FileUuid) of
        {ok, #document{value = #custom_metadata{value = Meta}}} ->
            Keys = maps:keys(Meta),
            {ok, Keys};
        {error, not_found} ->
            {ok, []};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes extended attribute metadata.
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr_metadata(file_meta:uuid(), xattr:name()) ->
    ok | {error, term()}.
remove_xattr_metadata(FileUuid, Name) ->
    Diff = fun(Meta = #custom_metadata{value = MetaValue}) ->
        {ok, Meta#custom_metadata{value = maps:remove(Name, MetaValue)}}
    end,
    case datastore_model:update(?CTX, FileUuid, Diff) of
        {ok, _} -> ok;
        {error, not_found} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if extended attribute metadata exists.
%% @end
%%--------------------------------------------------------------------
-spec exists_xattr_metadata(file_meta:uuid(), xattr:name()) -> boolean().
exists_xattr_metadata(FileUuid, Name) ->
    case datastore_model:get(?CTX, FileUuid) of
        {ok, #document{value = #custom_metadata{value = MetaValue}}} ->
            maps:is_key(Name, MetaValue);
        {error, not_found} ->
            false
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets extended attribute metadata.
%% @end
%%--------------------------------------------------------------------
-spec set_xattr_metadata(file_meta:uuid(), od_space:id(), xattr:name(),
    xattr:value(), Create :: boolean(), Replace :: boolean()) ->
    {ok, file_meta:uuid()} | {error, term()}.
set_xattr_metadata(FileUuid, SpaceId, Name, Value, Create, Replace) ->
    Diff = fun(Meta = #custom_metadata{value = MetaValue}) ->
        case {maps:is_key(Name, MetaValue), Create, Replace} of
            {true, true, _} ->
                {error, ?EEXIST};
            {false, _, true} ->
                {error, ?ENODATA};
            _ ->
                NewMetaValue = maps:put(Name, Value, MetaValue),
                {ok, Meta#custom_metadata{value = NewMetaValue}}
        end
    end,
    case Replace of
        true ->
            case datastore_model:update(?CTX, FileUuid, Diff) of
                {error, not_found} -> {error, ?ENODATA};
                Other -> Other
            end;
        false ->
            FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
            {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
            Default = #custom_metadata{
                space_id = SpaceId,
                file_objectid = FileObjectId,
                value = maps:put(Name, Value, #{})
            },
            datastore_model:update(
                ?CTX#{scope => SpaceId}, FileUuid, Diff, Default
            )
    end.

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

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {space_id, string},
        {value, {custom, {json_utils, encode, decode}}}
    ]};
get_record_struct(2) ->
    {record, [
        {space_id, string},
        {file_objectid, string},
        {value, {custom, {json_utils, encode, decode}}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, SpaceId, Value}) ->
    {2, #custom_metadata{
        space_id = SpaceId,
        file_objectid = undefined,
        value = Value
    }}.
