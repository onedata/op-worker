%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to archives.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_middleware_plugin).
-author("Jakub Kudzia").

-behaviour(middleware_router).
-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/errors.hrl").

%% middleware_router callbacks
-export([resolve_handler/3]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).


%%%===================================================================
%%% middleware_router callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_router} callback resolve_handler/3.
%% @end
%%--------------------------------------------------------------------
-spec resolve_handler(middleware:operation(), gri:aspect(), middleware:scope()) ->
    module() | no_return().
resolve_handler(create, instance, private) -> ?MODULE;
resolve_handler(create, cancel, private) -> ?MODULE;
resolve_handler(create, delete, private) -> ?MODULE;
resolve_handler(create, recall, private) -> ?MODULE;
resolve_handler(create, identify_file, private) -> ?MODULE;

resolve_handler(get, instance, private) -> ?MODULE;
resolve_handler(get, audit_log, private) -> ?MODULE;

resolve_handler(update, instance, private) -> ?MODULE;

resolve_handler(_, _, _) -> throw(?ERROR_NOT_SUPPORTED).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = instance}}) -> #{
    required => #{
        <<"datasetId">> => {binary, non_empty}
    },
    optional => #{
        <<"config">> => {json, fun(RawConfig) -> {true, archive_config:sanitize(RawConfig)} end},
        <<"description">> => {binary, any},
        <<"preservedCallback">> => {binary, fun(Callback) -> url_utils:is_valid(Callback) end},
        <<"deletedCallback">> => {binary, fun(Callback) -> url_utils:is_valid(Callback) end}
    }
};
data_spec(#op_req{operation = create, gri = #gri{aspect = cancel}}) ->
    undefined;
data_spec(#op_req{operation = create, gri = #gri{aspect = delete}}) -> #{
    optional => #{
        <<"deletedCallback">> => {binary, fun(Callback) -> url_utils:is_valid(Callback) end}
    }
};
data_spec(#op_req{operation = create, gri = #gri{aspect = recall}}) -> #{
    required => #{
        <<"parentDirectoryId">> => {binary,
            fun(ObjectId) -> {true, middleware_utils:decode_object_id(ObjectId, <<"parentDirectoryId">>)} end}
    },
    optional => #{
        <<"targetFileName">> => {binary, non_empty}
    }
};
data_spec(#op_req{operation = create, gri = #gri{aspect = identify_file}}) -> #{
    required => #{
        <<"relativePath">> => {binary, non_empty}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;
data_spec(#op_req{operation = get, gri = #gri{aspect = audit_log}}) ->
    audit_log_browse_opts:json_data_spec();

data_spec(#op_req{operation = update, gri = #gri{aspect = instance}}) -> #{
    optional => #{
        <<"description">> => {binary, any},
        <<"preservedCallback">> => {binary, fun(Callback) -> url_utils:is_valid(Callback) end},
        <<"deletedCallback">> => {binary, fun(Callback) -> url_utils:is_valid(Callback) end}
    }
}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(#op_req{auth = ?NOBODY}) ->
    ?ERROR_UNAUTHORIZED;

fetch_entity(#op_req{operation = Op, auth = ?USER(_UserId), gri = #gri{
    id = ArchiveId,
    aspect = As,
    scope = private
}}) when
    (Op =:= create andalso As =:= delete);
    (Op =:= create andalso As =:= cancel);
    (Op =:= create andalso As =:= recall);
    (Op =:= create andalso As =:= identify_file);
    (Op =:= get andalso As =:= instance);
    (Op =:= get andalso As =:= audit_log);
    (Op =:= update andalso As =:= instance)
->
    case archive:get(ArchiveId) of
        {ok, ArchiveDoc} ->
            {ok, {ArchiveDoc, 1}};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback authorize/2.
%%
%% Checks only membership in space. Archive management privileges
%% are checked later by fslogic layer.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{operation = create, auth = Auth, gri = #gri{aspect = instance}, data = Data}, _) ->
    DatasetId = maps:get(<<"datasetId">>, Data),
    {ok, SpaceId} = dataset:get_space_id(DatasetId),
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = Op, auth = Auth, gri = #gri{aspect = As}}, ArchiveDoc) when
    (Op =:= create andalso As =:= cancel);
    (Op =:= create andalso As =:= delete);
    (Op =:= create andalso As =:= recall);
    (Op =:= create andalso As =:= identify_file);
    (Op =:= get andalso As =:= instance);
    (Op =:= get andalso As =:= audit_log);
    (Op =:= update andalso As =:= instance)
->
    {ok, SpaceId} = archive:get_space_id(ArchiveDoc),
    middleware_utils:is_eff_space_member(Auth, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{aspect = instance}, data = Data}, _) ->
    DatasetId = maps:get(<<"datasetId">>, Data),
    {ok, SpaceId} = dataset:get_space_id(DatasetId),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = Op, gri = #gri{aspect = As}}, ArchiveDoc) when
    (Op =:= create andalso As =:= cancel);
    (Op =:= create andalso As =:= delete);
    (Op =:= create andalso As =:= recall);
    (Op =:= create andalso As =:= identify_file);
    (Op =:= get andalso As =:= instance);
    (Op =:= get andalso As =:=  audit_log);
    (Op =:= update andalso As =:= instance)
->
    {ok, SpaceId} = archive:get_space_id(ArchiveDoc),
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = Auth, data = Data, gri = #gri{aspect = instance} = GRI}) ->
    SessionId = Auth#auth.session_id,
    DatasetId = maps:get(<<"datasetId">>, Data),
    ConfigJson = maps:get(<<"config">>, Data, #{}),
    Config = archive_config:from_json(ConfigJson),
    Description = maps:get(<<"description">>, Data, ?DEFAULT_ARCHIVE_DESCRIPTION),
    PreservedCallback = maps:get(<<"preservedCallback">>, Data, undefined),
    DeletedCallback = maps:get(<<"deletedCallback">>, Data, undefined),
    ArchiveInfo = #archive_info{id = ArchiveId} = mi_archives:archive_dataset(
        SessionId, DatasetId, Config, PreservedCallback, DeletedCallback, Description
    ),
    {ok, resource, {GRI#gri{id = ArchiveId}, ArchiveInfo}};

create(#op_req{auth = Auth, gri = #gri{id = ArchiveId, aspect = cancel}, data = Data}) ->
    SessionId = Auth#auth.session_id,
    PreservationPolicy = case maps:get(<<"preservationPolicy">>, Data, <<"retain">>) of
        <<"retain">> -> retain;
        <<"delete">> -> delete
    end,
    mi_archives:cancel_archivisation(SessionId, ArchiveId, PreservationPolicy);

create(#op_req{auth = Auth, data = Data, gri = #gri{id = ArchiveId, aspect = delete}}) ->
    SessionId = Auth#auth.session_id,
    Callback = maps:get(<<"deletedCallback">>, Data, undefined),
    mi_archives:delete(SessionId, ArchiveId, Callback);

create(#op_req{auth = Auth, data = Data, gri = #gri{id = ArchiveId, aspect = recall}}) ->
    SessionId = Auth#auth.session_id,
    ParentDirectoryGuid = maps:get(<<"parentDirectoryId">>, Data),
    TargetFileName = maps:get(<<"targetFileName">>, Data, default),
    {ok, value, mi_archives:recall(SessionId, ArchiveId, ParentDirectoryGuid, TargetFileName)};

create(#op_req{auth = Auth, gri = #gri{id = ArchiveId, aspect = identify_file}, data = Data}) ->
    SessionId = Auth#auth.session_id,
    ArchiveInfo = mi_archives:get_info(SessionId, ArchiveId),
    DatasetInfo = mi_datasets:get_info(SessionId, ArchiveInfo#archive_info.dataset_id),
    ArchiveRelativePath = maps:get(<<"relativePath">>, Data),
    [_ | DatasetRelativePathTokens] = filename:split(ArchiveRelativePath),
    SourceFileGuid = case DatasetRelativePathTokens of
        [] ->
            DatasetInfo#dataset_info.root_file_guid;
        _ ->
            resolve_guid_by_relative_path(SessionId, DatasetInfo#dataset_info.root_file_guid,
                filename:join(DatasetRelativePathTokens))
    end,
    {ok, value, #{
        <<"archivedFile">> =>
            resolve_guid_by_relative_path(SessionId, ArchiveInfo#archive_info.data_dir_guid, ArchiveRelativePath),
        <<"sourceFile">> =>
            SourceFileGuid
    }}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, gri = #gri{id = ArchiveId, aspect = instance}}, _) ->
    {ok, mi_archives:get_info(Auth#auth.session_id, ArchiveId)};

get(#op_req{gri = #gri{id = ArchiveIdId, aspect = audit_log}, data = Data}, _) ->
    BrowseOpts = audit_log_browse_opts:from_json(Data),
    case archivisation_audit_log:browse(ArchiveIdId, BrowseOpts) of
        {ok, BrowseResult} ->
            {ok, value, BrowseResult};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{auth = Auth, gri = #gri{id = ArchiveId, aspect = instance}, data = Data}) ->
    mi_archives:update(Auth#auth.session_id, ArchiveId, Data).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{}) ->
    ?ERROR_NOT_SUPPORTED.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec resolve_guid_by_relative_path(session:id(), file_id:file_guid(), file_meta:path()) -> 
    file_id:file_guid() | undefined.
resolve_guid_by_relative_path(SessionId, RootFileGuid, RelativePath) ->
    case lfm:resolve_guid_by_relative_path(SessionId, RootFileGuid, RelativePath) of
        {ok, Guid} -> Guid;
        {error, ?ENOENT} -> undefined;
        {error, Errno} -> throw(?ERROR_POSIX(Errno))
    end.
