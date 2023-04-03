%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for holding files' custom metadata.
%%% Note: this module operates on referenced uuids - all operations on hardlinks
%%% are treated as operations on original file. Thus, custom_metadata is shared
%%% between hardlinks and original file.
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
-include_lib("ctool/include/errors.hrl").

%% API
-export([get/1, update/2, delete/1, create_or_update/2]).
-export([list_xattrs/1, get_xattr/2, get_all_xattrs/1, set_xattr/7, remove_xattr/2]).
-export([ensure_synced/1]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

% Metadata types
-type type() :: json | rdf.
-type rdf() :: binary().
-type query() :: json_utils:query().

% Cdmi metadata/attributes
-type transfer_encoding() :: binary(). % <<"utf-8">> | <<"base64">>
-type cdmi_completion_status() :: binary(). % <<"Completed">> | <<"Processing">> | <<"Error">>
-type mimetype() :: binary().
-type cdmi_metadata() :: transfer_encoding() | cdmi_completion_status() | mimetype().

-type name() :: binary().
-type value() :: binary() | rdf() | json_utils:json_term().

-type record() :: #custom_metadata{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-export_type([
    type/0, rdf/0, query/0,
    transfer_encoding/0, cdmi_completion_status/0, mimetype/0, cdmi_metadata/0,
    name/0, value/0,
    record/0, doc/0, diff/0
]).

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


-spec get(file_meta:uuid()) -> {ok, doc()} | {error, term()}.
get(FileUuid) ->
    datastore_model:get(?CTX, fslogic_file_id:ensure_referenced_uuid(FileUuid)).


-spec update(file_meta:uuid(), diff()) ->
    {ok, file_meta:uuid()} | {error, term()}.
update(FileUuid, Diff) ->
    ?extract_key(datastore_model:update(?CTX, fslogic_file_id:ensure_referenced_uuid(FileUuid), Diff)).


-spec delete(file_meta:uuid()) -> ok | {error, term()}.
delete(FileUuid) ->
    datastore_model:delete(?CTX, fslogic_file_id:ensure_referenced_uuid(FileUuid)).


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
        ?CTX#{scope => Scope}, fslogic_file_id:ensure_referenced_uuid(Key), Diff, Default)
    ).


-spec list_xattrs(file_meta:uuid()) -> {ok, [name()]} | {error, term()}.
list_xattrs(FileUuid) ->
    case datastore_model:get(?CTX, fslogic_file_id:ensure_referenced_uuid(FileUuid)) of
        {ok, #document{value = #custom_metadata{value = Metadata}}} ->
            {ok, maps:keys(Metadata)};
        {error, not_found} ->
            {ok, []};
        {error, Reason} ->
            {error, Reason}
    end.


-spec get_xattr(file_meta:uuid(), name()) -> {ok, value()} | {error, term()}.
get_xattr(FileUuid, Name) ->
    case datastore_model:get(?CTX, fslogic_file_id:ensure_referenced_uuid(FileUuid)) of
        {ok, #document{value = #custom_metadata{value = Metadata}}} ->
            case maps:find(Name, Metadata) of
                {ok, _} = Result ->
                    Result;
                error ->
                    {error, not_found}
            end;
        {error, _} = Error ->
            Error
    end.


-spec get_all_xattrs(file_meta:uuid()) -> {ok, #{name() => value()}} | {error, term()}.
get_all_xattrs(FileUuid) ->
    case datastore_model:get(?CTX, fslogic_file_id:ensure_referenced_uuid(FileUuid)) of
        {ok, #document{value = #custom_metadata{value = Metadata}}} ->
            {ok, Metadata};
        {error, not_found} ->
            {ok, #{}};
        {error, Reason} ->
            {error, Reason}
    end.


-spec set_xattr(
    file_meta:uuid(),
    od_space:id(),
    name(),
    value(),
    Create :: boolean(),
    Replace :: boolean(),
    IsIgnoredInChanges :: boolean()
) ->
    {ok, file_meta:uuid()} | {error, term()}.
set_xattr(FileUuid, SpaceId, Name, Value, Create, Replace, IsIgnoredInChanges) ->
    EffectiveFileUuid = fslogic_file_id:ensure_referenced_uuid(FileUuid),
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
            case ?extract_key(datastore_model:update(?CTX, EffectiveFileUuid, Diff)) of
                {error, not_found} ->
                    {error, ?ENODATA};
                Other ->
                    Other
            end;
        false ->
            FileGuid = file_id:pack_guid(EffectiveFileUuid, SpaceId),
            {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
            Default = #document{
                key = EffectiveFileUuid,
                value = #custom_metadata{
                    space_id = SpaceId,
                    file_objectid = FileObjectId,
                    value = #{Name => Value}
                },
                ignore_in_changes = IsIgnoredInChanges
            },
            ?extract_key(datastore_model:update(
                ?CTX#{scope => SpaceId}, EffectiveFileUuid, Diff, Default
            ))
    end.


-spec remove_xattr(file_meta:uuid(), name()) -> ok | {error, term()}.
remove_xattr(FileUuid, Name) ->
    Diff = fun(#custom_metadata{value = Metadata} = Record) ->
        case maps:take(Name, Metadata) of
            {_XattrValue, MetadataWithoutXattr} ->
                {ok, Record#custom_metadata{value = MetadataWithoutXattr}};
            error ->
                {error, not_found}
        end
    end,
    ?ok_if_not_found(?extract_ok(datastore_model:update(?CTX,
        fslogic_file_id:ensure_referenced_uuid(FileUuid), Diff))).


-spec ensure_synced(file_meta:uuid()) -> ok.
ensure_synced(Key) ->
    UpdateAns = datastore_model:update(?CTX#{ignore_in_changes => false}, Key, fun(Record) ->
        {ok, Record} % Return unchanged record, ignore_in_changes will be unset because of flag in CTX
    end),
    case UpdateAns of
        {ok, _} -> ok;
        {error, not_found} -> ok
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
    3.


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
        {value, {custom, json, {json_utils, encode, decode}}}
    ]};
get_record_struct(2) ->
    {record, [
        {space_id, string},
        {file_objectid, string},
        {value, {custom, json, {json_utils, encode, decode}}}
    ]};
get_record_struct(3) ->
    % In version 3 only acl was removed from metadata
    % but struct remains the same.
    get_record_struct(2).


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
    }};
upgrade_record(2, {?MODULE, SpaceId, ObjectId, Value}) ->
    {3, #custom_metadata{
        space_id = SpaceId,
        file_objectid = ObjectId,
        value = maps:remove(?ACL_KEY, Value)
    }}.
