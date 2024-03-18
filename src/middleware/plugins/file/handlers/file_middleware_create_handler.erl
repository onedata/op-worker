%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create) corresponding to file aspects such as:
%%% - attributes,
%%% - extended attributes,
%%% - json metadata,
%%% - rdf metadata.
%%% @end
%%%-------------------------------------------------------------------
-module(file_middleware_create_handler).
-author("Bartosz Walkowicz").

-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/privileges.hrl").


-export([assert_operation_supported/2]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).


%%%===================================================================
%%% API
%%%===================================================================

-spec assert_operation_supported(gri:aspect(), middleware:scope()) ->
    ok | no_return().
assert_operation_supported(instance, private)              -> ok;
assert_operation_supported(object_id, private)             -> ok;
assert_operation_supported(attrs, private)                 -> ok;    % REST/gs
assert_operation_supported(xattrs, private)                -> ok;    % REST/gs
assert_operation_supported(json_metadata, private)         -> ok;    % REST/gs
assert_operation_supported(rdf_metadata, private)          -> ok;    % REST/gs
assert_operation_supported(register_file, private)         -> ok;
assert_operation_supported(cancel_archive_recall, private) -> ok;
assert_operation_supported(_, _)                           -> throw(?ERROR_NOT_SUPPORTED).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================

-spec data_spec(middleware:req()) ->
    undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{gri = Gri, data = Data}) ->
    data_spec(Gri, Data).


%% @private
-spec data_spec(gri:gri(), middleware:data()) ->
    undefined | middleware_sanitizer:data_spec().
data_spec(#gri{aspect = instance}, Data) ->
    AlwaysRequired = #{
        <<"name">> => {binary, non_empty},
        <<"type">> => {atom, [
            ?REGULAR_FILE_TYPE, ?DIRECTORY_TYPE, ?LINK_TYPE, ?SYMLINK_TYPE
        ]},
        <<"parent">> => {binary, fun(Parent) ->
            try gri:deserialize(Parent) of
                #gri{type = op_file, id = ParentGuid, aspect = instance} ->
                    {true, ParentGuid};
                _ ->
                    throw(?ERROR_BAD_VALUE_IDENTIFIER(<<"parent">>))
            catch _:_ ->
                false
            end
        end}
    },
    AllOptional = #{
        <<"createAttempts">> => {integer, {between, 1, 200}},
        <<"responseAttributes">> => file_middleware_handlers_common_utils:build_attributes_param_spec(
            private, current, <<"responseAttributes">>)
    },

    AllRequired = case maps:get(<<"type">>, Data, undefined) of
        <<"LNK">> ->
            AlwaysRequired#{<<"targetGuid">> => {binary, guid}};
        <<"SYMLNK">> ->
            AlwaysRequired#{<<"targetPath">> => {binary, non_empty}};
        _ ->
            AlwaysRequired
    end,

    #{required => AllRequired, optional => AllOptional};

data_spec(#gri{aspect = object_id}, _) ->
    undefined;

data_spec(#gri{aspect = attrs}, _) ->
    ModeParam = <<"mode">>,

    #{
        required => #{
            id => {binary, guid},
            ModeParam => {binary, fun(Mode) ->
                try binary_to_integer(Mode, 8) of
                    ValidMode when ValidMode >= 0 andalso ValidMode =< 8#1777 ->
                        {true, ValidMode};
                    _ ->
                        throw(?ERROR_BAD_VALUE_NOT_IN_RANGE(ModeParam, 0, 8#1777))
                catch _:_ ->
                    throw(?ERROR_BAD_VALUE_INTEGER(ModeParam))
                end
            end}
        }
    };

data_spec(#gri{aspect = xattrs}, _) -> #{
    required => #{
        id => {binary, guid},
        <<"metadata">> => {json, any}
    },
    optional => #{
        <<"resolve_symlink">> => {boolean, any}
    }
};

data_spec(#gri{aspect = json_metadata}, _) -> #{
    required => #{
        id => {binary, guid},
        <<"metadata">> => {any, any}
    },
    optional => #{
        <<"filter_type">> => {binary, [<<"keypath">>]},
        <<"filter">> => {binary, any},
        <<"resolve_symlink">> => {boolean, any}
    }
};

data_spec(#gri{aspect = rdf_metadata}, _) -> #{
    required => #{
        id => {binary, guid},
        <<"metadata">> => {binary, any}
    },
    optional => #{
        <<"resolve_symlink">> => {boolean, any}
    }
};

data_spec(#gri{aspect = register_file}, _) -> #{
    required => #{
        <<"spaceId">> => {binary, non_empty},
        <<"storageId">> => {binary, non_empty},
        <<"storageFileId">> => {binary, non_empty},
        <<"destinationPath">> => {binary, non_empty}
    },
    optional => #{
        <<"size">> => {integer, {not_lower_than, 0}},
        <<"mode">> => {binary, octal},
        <<"atime">> => {integer, {not_lower_than, 0}},
        <<"mtime">> => {integer, {not_lower_than, 0}},
        <<"ctime">> => {integer, {not_lower_than, 0}},
        <<"uid">> => {integer, {between, 0, ?UID_MAX}},
        <<"gid">> => {integer, {between, 0, ?GID_MAX}},
        <<"autoDetectAttributes">> => {boolean, any},
        <<"xattrs">> => {json, any},
        <<"json">> => {json, any},
        <<"rdf">> => {binary, any}
    }
};

data_spec(#gri{aspect = cancel_archive_recall}, _) ->
    undefined.


-spec fetch_entity(middleware:req()) -> {ok, middleware:versioned_entity()}.
fetch_entity(_) ->
    {ok, {undefined, 1}}.


-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = Auth, data = Data, gri = #gri{aspect = instance}}, _) ->
    SpaceId = file_id:guid_to_space_id(maps:get(<<"parent">>, Data)),
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{auth = Auth, gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata;
    As =:= cancel_archive_recall
->
    middleware_utils:has_access_to_file_space(Auth, Guid);

authorize(#op_req{gri = #gri{aspect = object_id}}, _) ->
    % File path must have been resolved to guid by rest_handler already (to
    % get to this point), so authorization is surely granted.
    true;
authorize(#op_req{auth = Auth = ?USER(UserId), data = Data, gri = #gri{aspect = register_file}}, _) ->
    SpaceId = maps:get(<<"spaceId">>, Data),
    middleware_utils:is_eff_space_member(Auth, SpaceId) andalso
        space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_REGISTER_FILES).


-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{data = Data, gri = #gri{aspect = instance}}, _) ->
    SpaceId = file_id:guid_to_space_id(maps:get(<<"parent">>, Data)),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata;
    As =:= cancel_archive_recall
->
    middleware_utils:assert_file_managed_locally(Guid);

validate(#op_req{gri = #gri{aspect = object_id}}, _) ->
    % File path must have been resolved to guid by rest_handler already (to
    % get to this point), so file must be managed locally.
    ok;

validate(#op_req{data = Data, gri = #gri{aspect = register_file}}, _) ->
    SpaceId = maps:get(<<"spaceId">>, Data),
    StorageId = maps:get(<<"storageId">>, Data),
    middleware_utils:assert_space_supported_locally(SpaceId),
    middleware_utils:assert_space_supported_with_storage(SpaceId, StorageId),
    storage_import:assert_imported_storage(StorageId).


-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = Auth, data = Data, gri = #gri{aspect = instance} = GRI}) ->
    SessionId = Auth#auth.session_id,
    FileType = maps:get(<<"type">>, Data),

    Target = case FileType of
        ?LINK_TYPE -> maps:get(<<"targetGuid">>, Data);
        ?SYMLINK_TYPE -> maps:get(<<"targetPath">>, Data);
        _ -> undefined
    end,

    {ok, Guid} = create_file(
        SessionId,
        maps:get(<<"parent">>, Data),
        maps:get(<<"name">>, Data),
        FileType, Target,
        0,
        maps:get(<<"createAttempts">>, Data, 1)
    ),

    RequestedAttrs = maps:get(<<"responseAttributes">>, Data, ?API_ATTRS),
    {ok, FileAttr} = ?lfm_check(lfm:stat(SessionId, ?FILE_REF(Guid), RequestedAttrs)),
    {ok, resource, {GRI#gri{id = Guid}, file_attr_translator:to_json(FileAttr, current, RequestedAttrs)}};

create(#op_req{gri = #gri{id = FileGuid, aspect = object_id}}) ->
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    {ok, value, ObjectId};

create(#op_req{auth = Auth, data = Data, gri = #gri{id = Guid, aspect = attrs}}) ->
    Mode = maps:get(<<"mode">>, Data),
    ?lfm_check(lfm:set_perms(Auth#auth.session_id, ?FILE_REF(Guid), Mode));

create(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = xattrs}}) ->
    SessionId = Auth#auth.session_id,
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),

    lists:foreach(fun({XattrName, XattrValue}) ->
        Xattr = #xattr{name = XattrName, value = XattrValue},
        ?lfm_check(lfm:set_xattr(SessionId, FileRef, Xattr, false, false))
    end, maps:to_list(maps:get(<<"metadata">>, Data)));

create(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = json_metadata}}) ->
    SessionId = Auth#auth.session_id,
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),

    JSON = maps:get(<<"metadata">>, Data),
    Filter = maps:get(<<"filter">>, Data, undefined),
    FilterType = maps:get(<<"filter_type">>, Data, undefined),
    FilterList = case {FilterType, Filter} of
        {undefined, _} ->
            [];
        {<<"keypath">>, undefined} ->
            throw(?ERROR_MISSING_REQUIRED_VALUE(<<"filter">>));
        {<<"keypath">>, _} ->
            binary:split(Filter, <<".">>, [global])
    end,

    mi_file_metadata:set_custom_metadata(SessionId, FileRef, json, JSON, FilterList);

create(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = rdf_metadata}}) ->
    SessionId = Auth#auth.session_id,
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),

    Rdf = maps:get(<<"metadata">>, Data),

    mi_file_metadata:set_custom_metadata(SessionId, FileRef, rdf, Rdf, []);

create(#op_req{auth = Auth, data = Data, gri = #gri{aspect = register_file}}) ->
    SpaceId = maps:get(<<"spaceId">>, Data),
    DestinationPath = maps:get(<<"destinationPath">>, Data),
    StorageId = maps:get(<<"storageId">>, Data),
    StorageFileId = maps:get(<<"storageFileId">>, Data),
    try
        case file_registration:register(Auth#auth.session_id, SpaceId, DestinationPath, StorageId, StorageFileId, Data) of
            {ok, FileGuid} ->
                {ok, FileId} = file_id:guid_to_objectid(FileGuid),
                {ok, value, FileId};
            Error2 ->
                throw(Error2)
        end
    catch
        throw:{error, _} = Error ->
            throw(Error);
        throw:PosixErrno ->
            throw(?ERROR_POSIX(PosixErrno))
    end;

create(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = cancel_archive_recall}}) ->
    SessionId = Auth#auth.session_id,
    mi_archives:cancel_recall(SessionId, FileGuid).


%% @doc {@link middleware_handler} callback get/2.
-spec get(middleware:req(), middleware:entity()) -> no_return().
get(_, _) ->
    error(not_implemented).


%% @doc {@link middleware_handler} callback update/1.
-spec update(middleware:req()) -> no_return().
update(_) ->
    error(not_implemented).


%% @doc {@link middleware_handler} callback delete/1.
-spec delete(middleware:req()) -> no_return().
delete(_) ->
    error(not_implemented).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec create_file(
    SessionId :: session:id(),
    ParentGuid :: file_id:file_guid(),
    Name :: file_meta:name(),
    Type :: file_meta:type(),
    Target :: undefined | file_id:file_guid() | file_meta:path(),
    Counter :: non_neg_integer(),
    Attempts :: non_neg_integer()
) ->
    {ok, file_id:file_guid()} | no_return().
create_file(_, _, _, _, _, Counter, Attempts) when Counter >= Attempts ->
    throw(?ERROR_POSIX(?EEXIST));
create_file(SessId, ParentGuid, OriginalName, Type, Target, Counter, Attempts) ->
    Name = maybe_add_file_suffix(OriginalName, Counter),
    case create_file(SessId, ParentGuid, Name, Type, Target) of
        {error, ?EEXIST} ->
            create_file(
                SessId, ParentGuid, OriginalName, Type, Target,
                Counter + 1, Attempts
            );
        {ok, #file_attr{guid = FileGuid}} ->
            {ok, FileGuid};
        Result ->
            ?lfm_check(Result)
    end.


%% @private
-spec maybe_add_file_suffix(file_meta:name(), Counter :: non_neg_integer()) ->
    file_meta:name().
maybe_add_file_suffix(OriginalName, 0) ->
    OriginalName;
maybe_add_file_suffix(OriginalName, Counter) ->
    RootName = filename:rootname(OriginalName),
    Ext = filename:extension(OriginalName),
    str_utils:format_bin("~ts(~B)~ts", [RootName, Counter, Ext]).


%% @private
-spec create_file(
    SessionId :: session:id(),
    ParentGuid :: file_id:file_guid(),
    Name :: file_meta:name(),
    Type :: file_meta:type(),
    Target :: undefined | file_id:file_guid() | file_meta:path()
) ->
    {ok, file_id:file_guid() | lfm_attrs:file_attributes()} | {error, term()}.
create_file(SessionId, ParentGuid, Name, ?REGULAR_FILE_TYPE, undefined) ->
    lfm:create(SessionId, ParentGuid, Name, undefined);
create_file(SessionId, ParentGuid, Name, ?DIRECTORY_TYPE, undefined) ->
    lfm:mkdir(SessionId, ParentGuid, Name, undefined);
create_file(SessionId, ParentGuid, Name, ?LINK_TYPE, TargetGuid) ->
    lfm:make_link(SessionId, ?FILE_REF(TargetGuid), ?FILE_REF(ParentGuid), Name);
create_file(SessionId, ParentGuid, Name, ?SYMLINK_TYPE, TargetPath) ->
    lfm:make_symlink(SessionId, ?FILE_REF(ParentGuid), Name, TargetPath).

