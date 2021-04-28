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
-export([create/5, get/1, update/2, delete/1]).

% getters
-export([get_id/1, get_creation_time/1, get_dataset_id/1, get_root_dir/1, get_space_id/1,
    get_state/1, get_type/1, get_character/1, get_data_structure/1, get_metadata_structure/1,
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
-type character() :: ?DIP | ?AIP | ?HYBRID.
-type data_structure() :: ?BAGIT | ?SIMPLE_COPY.
-type metadata_structure() :: ?BUILT_IN | ?JSON | ?XML.
-type timestamp() :: time:seconds().
-type description() :: binary().

% @formatter:on
-type params() :: #{
    type := type(),
    character := character(),
    data_structure := data_structure(),
    metadata_structure := metadata_structure()
}.

-type attrs() :: #{
    description => description()
}.
% @formatter:off

-type error() :: {error, term()}.

-export_type([
    id/0, doc/0,
    creator/0, type/0, state/0, character/0,
    data_structure/0, metadata_structure/0,
    timestamp/0, description/0, params/0, attrs/0
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

-spec create(dataset:id(), od_space:id(), creator(), params(), attrs()) -> {ok, doc()} | error().
create(DatasetId, SpaceId, Creator, Params, Attrs) ->
    SanitizedParams = sanitize_params(Params),
    SanitizedAttrs = sanitize_attrs(Attrs),
    datastore_model:create(?CTX, #document{
        value = #archive{
            dataset_id = DatasetId,
            creation_time = global_clock:timestamp_seconds(),
            creator = Creator,
            type = maps:get(type, SanitizedParams),
            state = ?EMPTY,
            character = maps:get(character, SanitizedParams),
            data_structure = maps:get(data_structure, SanitizedParams),
            metadata_structure = maps:get(metadata_structure, SanitizedParams),
            description = maps:get(description, SanitizedAttrs, <<>>)
        },
        scope = SpaceId
    }).


-spec update(id(), attrs()) -> ok | error().
update(ArchiveId, Attrs) ->
    SanitizedAttrs = sanitize_attrs(Attrs),
    ?extract_ok(datastore_model:update(?CTX, ArchiveId, fun(Archive = #archive{description = CurrentDescription}) ->
        {ok, Archive#archive{
            % TODO VFS-7616 which attrs can be updated?
            description = maps:get(description, SanitizedAttrs, CurrentDescription)
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

-spec get_creation_time(record() | doc()) -> timestamp().
get_creation_time(#archive{creation_time = CreationTime}) ->
    CreationTime;
get_creation_time(#document{value = Archive}) ->
    get_creation_time(Archive).

-spec get_dataset_id(record() | doc()) -> dataset:id().
get_dataset_id(#archive{dataset_id = DatasetId}) ->
    DatasetId;
get_dataset_id(#document{value = Archive}) ->
    get_dataset_id(Archive).

-spec get_root_dir(record() | doc()) -> file_id:file_guid() | undefined.
get_root_dir(#archive{root_dir_guid = RootDir}) ->
    RootDir;
get_root_dir(#document{value = Archive}) ->
    get_root_dir(Archive).

-spec get_space_id(doc()) -> od_space:id().
get_space_id(#document{scope = SpaceId}) ->
    SpaceId.

-spec get_state(record() | doc()) -> state().
get_state(#archive{state = State}) ->
    State;
get_state(#document{value = Archive}) ->
    get_state(Archive).

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
    % todo usunac stad?
    middleware_sanitizer:sanitize_data(Params, #{
        required => #{
            type => {atom, [?FULL_ARCHIVE, ?INCREMENTAL_ARCHIVE]},
            character => {atom, [?DIP, ?AIP, ?HYBRID]},
            data_structure => {atom, [?BAGIT, ?SIMPLE_COPY]},
            metadata_structure => {atom, [?BUILT_IN, ?JSON, ?XML]}
        },
        optional => #{
            description => {binary, any}
        }
    }).


-spec sanitize_attrs(attrs()) -> attrs().
sanitize_attrs(Attrs) ->
    middleware_sanitizer:sanitize_data(Attrs, #{
        optional => #{description => {binary, any}}
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
        {root_dir_guid, string},
        {creation_time, integer},
        {creator, string},
        {type, atom},
        {state, atom},
        {character, atom},
        {data_structure, atom},
        {metadata_structure, atom},
        {description, binary}
    ]}.
