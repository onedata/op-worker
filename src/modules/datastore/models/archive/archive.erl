%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for storing information about archives.
%%% @end
%%%-------------------------------------------------------------------
-module(archive).
-author("Jakub Kudzia").

-include("modules/archive/archive.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([create/4, get/1, update/2, delete/1]).

% getters
-export([get_id/1, get_timestamp/1, get_dataset_id/1, get_root_dir/1, get_space_id/1,
    get_type/1, get_character/1, get_data_structure/1, get_metadata_structure/1,
    get_description/1
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-compile([{no_auto_import, [get/1]}]).

-type id() :: binary().
-type record() :: #archive{}.
-type doc() :: datastore_doc:doc(record()).

-type creator() :: od_user:id().
-type type() :: ?FULL_ARCHIVE | ?INCREMENTAL_ARCHIVE.
-type state() :: ?EMPTY | ?INITIALIZING | ?ARCHIVED | ?RETIRING.
-type character() :: ?DIP | ?AIP | ?DIP_AIP.
-type data_structure() :: ?BAGIT | ?SIMPLE_COPY.
-type metadata_structure() :: ?BUILT_IN | ?JSON | ?XML.
-type timestamp() :: time:seconds().
-type description() :: binary().

% @formatter:on
-type params() :: #{
    type => type(),
    character => character(),
    data_structure => data_structure(),
    metadata_structure => metadata_structure(),
    description => description()
}.
% @formatter:off

-type error() :: {error, term()}.

-export_type([
    id/0, doc/0,
    creator/0, type/0, state/0, character/0,
    data_structure/0, metadata_structure/0,
    timestamp/0, description/0, params/0
]).

% @formatter:on
-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).
% @formatter:off


%%%===================================================================
%%% API functions
%%%===================================================================

-spec create(dataset:id(), od_space:id(), creator(), params()) -> {ok, doc()} | error().
create(DatasetId, SpaceId, Creator, Params) ->
    SanitizedParams = sanitize_params(Params),
    datastore_model:create(?CTX, #document{
        value = #archive{
            dataset_id = DatasetId,
            creation_timestamp = global_clock:timestamp_seconds(),
            creator = Creator,
            type = maps:get(type, SanitizedParams),
            state = ?EMPTY,
            character = maps:get(character, SanitizedParams),
            data_structure = maps:get(data_structure, SanitizedParams),
            metadata_structure = maps:get(metadata_structure, SanitizedParams),
            description = maps:get(description, SanitizedParams, <<>>)
        },
        scope = SpaceId
    }).


-spec update(id(), params()) -> ok | error().
update(ArchiveId, Params) ->
    % TODO VFS-7616 sanitize params?
    ?extract_ok(datastore_model:update(?CTX, ArchiveId, fun(Archive = #archive{description = CurrentDescription}) ->
        {ok, Archive#archive{
            % TODO VFS-7616 which params can be updated?
            description = maps:get(description, Params, CurrentDescription)
        }}
    end)).


-spec get(id()) -> {ok, doc()} | error().
get(ArchiveId) ->
    datastore_model:get(?CTX, ArchiveId).


-spec delete(archive:id()) -> ok | error().
delete(ArchiveId) ->
    datastore_model:delete(?CTX, ArchiveId).

%%%===================================================================
%%% Getters/setters for #archive record
%%%===================================================================

-spec get_id(doc()) -> id().
get_id(#document{key = ArchiveId}) ->
    ArchiveId.

-spec get_timestamp(record() | doc()) -> timestamp().
get_timestamp(#archive{creation_timestamp = CreationTimestamp}) ->
    CreationTimestamp;
get_timestamp(#document{value = Archive}) ->
    get_timestamp(Archive).

-spec get_dataset_id(record() | doc()) -> dataset:id().
get_dataset_id(#archive{dataset_id = DatasetId}) ->
    DatasetId;
get_dataset_id(#document{value = Archive}) ->
    get_dataset_id(Archive).

-spec get_root_dir(record() | doc()) -> file_id:file_guid() | undefined.
get_root_dir(#archive{root_dir = RootDir}) ->
    RootDir;
get_root_dir(#document{value = Archive}) ->
    get_root_dir(Archive).

-spec get_space_id(doc()) -> od_space:id().
get_space_id(#document{scope = SpaceId}) ->
    SpaceId.

-spec get_type(record() | doc()) -> type().
get_type(#archive{type = Type}) ->
    Type;
get_type(#document{value = Archive}) ->
    get_type(Archive).

-spec get_character(record() | doc()) -> character().
get_character(#archive{character = Character}) ->
    Character;
get_character(#document{value = Archive}) ->
    get_character(Archive).

-spec get_data_structure(record() | doc()) -> data_structure().
get_data_structure(#archive{data_structure = DataStructure}) ->
    DataStructure;
get_data_structure(#document{value = Archive}) ->
    get_data_structure(Archive).

-spec get_metadata_structure(record() | doc()) -> metadata_structure().
get_metadata_structure(#archive{metadata_structure = MetadataStructure}) ->
    MetadataStructure;
get_metadata_structure(#document{value = Archive}) ->
    get_metadata_structure(Archive).

-spec get_description(record() | doc()) -> description().
get_description(#archive{description = Description}) ->
    Description;
get_description(#document{value = Archive}) ->
    get_description(Archive).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec sanitize_params(params()) -> params().
sanitize_params(Params) ->
    middleware_sanitizer:sanitize_data(Params, #{
        required => #{
            type => {atom, [?FULL_ARCHIVE, ?INCREMENTAL_ARCHIVE]},
            character => {atom, [?DIP, ?AIP, ?DIP_AIP]},
            data_structure => {atom, [?BAGIT, ?SIMPLE_COPY]},
            metadata_structure => {atom, [?BUILT_IN, ?JSON, ?XML]}
        },
        optional => #{
            description => {binary, any}
        }
    }).

%%%===================================================================
%%% Datastore callbacks
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
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {dataset_id, string},
        {root_dir, string},
        {creation_timestamp, integer},
        {creator, string},
        {type, atom},
        {state, atom},
        {character, atom},
        {data_structure, atom},
        {metadata_structure, atom},
        {description, binary}
    ]}.
