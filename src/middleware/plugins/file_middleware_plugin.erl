%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to file aspects such as:
%%% - attributes,
%%% - extended attributes,
%%% - json metadata,
%%% - rdf metadata.
%%% @end
%%%-------------------------------------------------------------------
-module(file_middleware_plugin).
-author("Bartosz Walkowicz").

-behaviour(middleware_router).
-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([file_attrs_to_json/1]).

%% middleware_router callbacks
-export([resolve_handler/3]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).


-define(DEFAULT_LIST_OFFSET, 0).
-define(DEFAULT_LIST_ENTRIES, 1000).


%%%===================================================================
%%% API
%%%===================================================================


-spec file_attrs_to_json(lfm_attrs:file_attributes()) -> json_utils:json_map().
file_attrs_to_json(#file_attr{
    guid = Guid,
    name = Name,
    mode = Mode,
    parent_guid = ParentGuid,
    uid = Uid,
    gid = Gid,
    atime = Atime,
    mtime = Mtime,
    ctime = Ctime,
    type = Type,
    size = Size,
    shares = Shares,
    provider_id = ProviderId,
    owner_id = OwnerId,
    nlink = HardlinksCount
}) ->
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),

    #{
        <<"file_id">> => ObjectId,
        <<"name">> => Name,
        <<"mode">> => <<"0", (integer_to_binary(Mode, 8))/binary>>,
        <<"parent_id">> => case ParentGuid of
            undefined ->
                null;
            _ ->
                {ok, ParentObjectId} = file_id:guid_to_objectid(ParentGuid),
                ParentObjectId
        end,
        <<"storage_user_id">> => Uid,
        <<"storage_group_id">> => Gid,
        <<"atime">> => Atime,
        <<"mtime">> => Mtime,
        <<"ctime">> => Ctime,
        <<"type">> => str_utils:to_binary(Type),
        <<"size">> => utils:null_to_undefined(Size),
        <<"shares">> => Shares,
        <<"provider_id">> => ProviderId,
        <<"owner_id">> => OwnerId,
        <<"hardlinks_count">> => utils:undefined_to_null(HardlinksCount)
    }.


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
resolve_handler(create, Aspect, Scope) ->
    resolve_create_operation_handler(Aspect, Scope);
resolve_handler(get, Aspect, Scope) ->
    resolve_get_operation_handler(Aspect, Scope);
resolve_handler(update, Aspect, Scope) ->
    resolve_update_operation_handler(Aspect, Scope);
resolve_handler(delete, Aspect, Scope) ->
    resolve_delete_operation_handler(Aspect, Scope).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, data = Data, gri = GRI}) ->
    data_spec_create(GRI, Data);
data_spec(#op_req{operation = get, gri = GRI}) ->
    data_spec_get(GRI);
data_spec(#op_req{operation = update, gri = GRI}) ->
    data_spec_update(GRI);
data_spec(#op_req{operation = delete, gri = GRI}) ->
    data_spec_delete(GRI).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) -> {ok, middleware:versioned_entity()}.
fetch_entity(_) ->
    {ok, {undefined, 1}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback authorize/2.
%%
%% Checks only if user is in space in which file exists. File permissions
%% are checked later by logical_file_manager (lfm).
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{operation = create} = Req, Entity) ->
    authorize_create(Req, Entity);
authorize(#op_req{operation = get} = Req, Entity) ->
    authorize_get(Req, Entity);
authorize(#op_req{operation = update} = Req, Entity) ->
    authorize_update(Req, Entity);
authorize(#op_req{operation = delete} = Req, Entity) ->
    authorize_delete(Req, Entity).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create} = Req, Entity) ->
    validate_create(Req, Entity);
validate(#op_req{operation = get} = Req, Entity) ->
    validate_get(Req, Entity);
validate(#op_req{operation = update} = Req, Entity) ->
    validate_update(Req, Entity);
validate(#op_req{operation = delete} = Req, Entity) ->
    validate_delete(Req, Entity).


%%%===================================================================
%%% CREATE SECTION
%%%===================================================================


%% @private
-spec resolve_create_operation_handler(gri:aspect(), middleware:scope()) ->
    module() | no_return().
resolve_create_operation_handler(instance, private) -> ?MODULE;
resolve_create_operation_handler(object_id, private) -> ?MODULE;
resolve_create_operation_handler(attrs, private) -> ?MODULE;                 % REST/gs
resolve_create_operation_handler(xattrs, private) -> ?MODULE;                % REST/gs
resolve_create_operation_handler(json_metadata, private) -> ?MODULE;         % REST/gs
resolve_create_operation_handler(rdf_metadata, private) -> ?MODULE;          % REST/gs
resolve_create_operation_handler(register_file, private) -> ?MODULE;
resolve_create_operation_handler(_, _) -> throw(?ERROR_NOT_SUPPORTED).


%% @private
-spec data_spec_create(gri:gri(), middleware:data()) ->
    undefined | middleware_sanitizer:data_spec().
data_spec_create(#gri{aspect = instance}, Data) ->
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
    AllOptional = #{<<"createAttempts">> => {integer, {between, 1, 200}}},

    AllRequired = case maps:get(<<"type">>, Data, undefined) of
        <<"LNK">> ->
            AlwaysRequired#{<<"targetGuid">> => {binary, guid}};
        <<"SYMLNK">> ->
            AlwaysRequired#{<<"targetPath">> => {binary, non_empty}};
        _ ->
            AlwaysRequired
    end,

    #{required => AllRequired, optional => AllOptional};

data_spec_create(#gri{aspect = object_id}, _) ->
    undefined;

data_spec_create(#gri{aspect = attrs}, _) ->
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

data_spec_create(#gri{aspect = xattrs}, _) -> #{
    required => #{
        id => {binary, guid},
        <<"metadata">> => {json, any}
    },
    optional => #{
        <<"resolve_symlink">> => {boolean, any}
    }
};

data_spec_create(#gri{aspect = json_metadata}, _) -> #{
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

data_spec_create(#gri{aspect = rdf_metadata}, _) -> #{
    required => #{
        id => {binary, guid},
        <<"metadata">> => {binary, any}
    },
    optional => #{
        <<"resolve_symlink">> => {boolean, any}
    }
};

data_spec_create(#gri{aspect = register_file}, _) -> #{
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
        <<"uid">> => {integer, {not_lower_than, 0}},
        <<"gid">> => {integer, {not_lower_than, 0}},
        <<"autoDetectAttributes">> => {boolean, any},
        <<"xattrs">> => {json, any},
        <<"json">> => {json, any},
        <<"rdf">> => {binary, any}
    }
}.


%% @private
-spec authorize_create(middleware:req(), middleware:entity()) -> boolean().
authorize_create(#op_req{auth = Auth, data = Data, gri = #gri{aspect = instance}}, _) ->
    SpaceId = file_id:guid_to_space_id(maps:get(<<"parent">>, Data)),
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize_create(#op_req{auth = Auth, gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata
->
    middleware_utils:has_access_to_file_space(Auth, Guid);

authorize_create(#op_req{gri = #gri{aspect = object_id}}, _) ->
    % File path must have been resolved to guid by rest_handler already (to
    % get to this point), so authorization is surely granted.
    true;
authorize_create(#op_req{auth = Auth = ?USER(UserId), data = Data, gri = #gri{aspect = register_file}}, _) ->
    SpaceId = maps:get(<<"spaceId">>, Data),
    middleware_utils:is_eff_space_member(Auth, SpaceId) andalso
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_REGISTER_FILES).


%% @private
-spec validate_create(middleware:req(), middleware:entity()) -> ok | no_return().
validate_create(#op_req{data = Data, gri = #gri{aspect = instance}}, _) ->
    SpaceId = file_id:guid_to_space_id(maps:get(<<"parent">>, Data)),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate_create(#op_req{gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata
->
    middleware_utils:assert_file_managed_locally(Guid);

validate_create(#op_req{gri = #gri{aspect = object_id}}, _) ->
    % File path must have been resolved to guid by rest_handler already (to
    % get to this point), so file must be managed locally.
    ok;

validate_create(#op_req{data = Data, gri = #gri{aspect = register_file}}, _) ->
    SpaceId = maps:get(<<"spaceId">>, Data),
    StorageId = maps:get(<<"storageId">>, Data),
    middleware_utils:assert_space_supported_locally(SpaceId),
    middleware_utils:assert_space_supported_with_storage(SpaceId, StorageId),
    storage_import:assert_imported_storage(StorageId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
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

    {ok, FileDetails} = ?lfm_check(lfm:get_details(SessionId, ?FILE_REF(Guid))),
    {ok, resource, {GRI#gri{id = Guid}, FileDetails}};

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

    ?lfm_check(lfm:set_metadata(SessionId, FileRef, json, JSON, FilterList));

create(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = rdf_metadata}}) ->
    SessionId = Auth#auth.session_id,
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),

    Rdf = maps:get(<<"metadata">>, Data),

    ?lfm_check(lfm:set_metadata(SessionId, FileRef, rdf, Rdf, []));

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
    end.

%%%===================================================================
%%% GET SECTION
%%%===================================================================


-spec resolve_get_operation_handler(gri:aspect(), middleware:scope()) ->
    module() | no_return().
resolve_get_operation_handler(instance, private) -> ?MODULE;             % gs only
resolve_get_operation_handler(instance, public) -> ?MODULE;              % gs only
resolve_get_operation_handler(children, private) -> ?MODULE;             % REST/gs
resolve_get_operation_handler(children, public) -> ?MODULE;              % REST/gs
resolve_get_operation_handler(children_details, private) -> ?MODULE;     % gs only
resolve_get_operation_handler(children_details, public) -> ?MODULE;      % gs only
resolve_get_operation_handler(attrs, private) -> ?MODULE;                % REST/gs
resolve_get_operation_handler(attrs, public) -> ?MODULE;                 % REST/gs
resolve_get_operation_handler(xattrs, private) -> ?MODULE;               % REST/gs
resolve_get_operation_handler(xattrs, public) -> ?MODULE;                % REST/gs
resolve_get_operation_handler(json_metadata, private) -> ?MODULE;        % REST/gs
resolve_get_operation_handler(json_metadata, public) -> ?MODULE;         % REST/gs
resolve_get_operation_handler(rdf_metadata, private) -> ?MODULE;         % REST/gs
resolve_get_operation_handler(rdf_metadata, public) -> ?MODULE;          % REST/gs
resolve_get_operation_handler(distribution, private) -> ?MODULE;         % REST/gs
resolve_get_operation_handler(acl, private) -> ?MODULE;
resolve_get_operation_handler(shares, private) -> ?MODULE;               % gs only
resolve_get_operation_handler(transfers, private) -> ?MODULE;
resolve_get_operation_handler(qos_summary, private) -> ?MODULE;          % REST/gs
resolve_get_operation_handler(dataset_summary, private) -> ?MODULE;
resolve_get_operation_handler(download_url, private) -> ?MODULE;         % gs only
resolve_get_operation_handler(download_url, public) -> ?MODULE;          % gs only
resolve_get_operation_handler(hardlinks, private) -> ?MODULE;
resolve_get_operation_handler({hardlinks, _}, private) -> ?MODULE;
resolve_get_operation_handler(symlink_value, public) -> ?MODULE;
resolve_get_operation_handler(symlink_value, private) -> ?MODULE;
resolve_get_operation_handler(symlink_target, public) -> ?MODULE;
resolve_get_operation_handler(symlink_target, private) -> ?MODULE;
resolve_get_operation_handler(archive_recall_details, private) -> ?MODULE;
resolve_get_operation_handler(archive_recall_progress, private) -> ?MODULE;
resolve_get_operation_handler(_, _) -> throw(?ERROR_NOT_SUPPORTED).


%% @private
-spec data_spec_get(gri:gri()) -> undefined | middleware_sanitizer:data_spec().
data_spec_get(#gri{aspect = instance}) -> #{
    required => #{id => {binary, guid}}
};

data_spec_get(#gri{aspect = As}) when
    As =:= children;
    As =:= children_details
-> #{
    required => #{id => {binary, guid}},
    optional => #{
        <<"limit">> => {integer, {between, 1, 1000}},
        <<"index">> => {binary, fun
            (null) ->
                {true, undefined};
            (undefined) ->
                true;
            (<<>>) ->
                throw(?ERROR_BAD_VALUE_EMPTY(<<"index">>));
            (IndexBin) when is_binary(IndexBin) ->
                true;
            (_) ->
                false
        end},
        <<"offset">> => {integer, any}
    }
};

data_spec_get(#gri{aspect = attrs, scope = private}) -> #{
    required => #{id => {binary, guid}},
    optional => #{<<"attribute">> => {binary, ?PRIVATE_BASIC_ATTRIBUTES}}
};
data_spec_get(#gri{aspect = attrs, scope = public}) -> #{
    required => #{id => {binary, guid}},
    optional => #{<<"attribute">> => {binary, ?PUBLIC_BASIC_ATTRIBUTES}}
};

data_spec_get(#gri{aspect = xattrs}) -> #{
    required => #{id => {binary, guid}},
    optional => #{
        <<"attribute">> => {binary, non_empty},
        <<"inherited">> => {boolean, any},
        <<"show_internal">> => {boolean, any},
        <<"resolve_symlink">> => {boolean, any}
    }
};

data_spec_get(#gri{aspect = json_metadata}) -> #{
    required => #{id => {binary, guid}},
    optional => #{
        <<"filter_type">> => {binary, [<<"keypath">>]},
        <<"filter">> => {binary, any},
        <<"inherited">> => {boolean, any},
        <<"resolve_symlink">> => {boolean, any}
    }
};

data_spec_get(#gri{aspect = rdf_metadata}) -> #{
    required => #{id => {binary, guid}},
    optional => #{<<"resolve_symlink">> => {boolean, any}}
};

data_spec_get(#gri{aspect = As}) when
    As =:= distribution;
    As =:= acl;
    As =:= shares;
    As =:= symlink_value;
    As =:= symlink_target;
    As =:= archive_recall_details;
    As =:= archive_recall_progress
->
    #{required => #{id => {binary, guid}}};

data_spec_get(#gri{aspect = hardlinks}) ->
    #{
        required => #{id => {binary, guid}},
        optional => #{<<"limit">> => {integer, {not_lower_than, 1}}}
    };

data_spec_get(#gri{aspect = {hardlinks, _}}) -> #{
    required => #{
        id => {binary, guid},
        {aspect, <<"guid">>} => {binary, guid}
    }
};

data_spec_get(#gri{aspect = transfers}) -> #{
    required => #{id => {binary, guid}},
    optional => #{<<"include_ended_ids">> => {boolean, any}}
};

data_spec_get(#gri{aspect = As}) when
    As =:= qos_summary;
    As =:= dataset_summary
-> #{
    required => #{id => {binary, guid}}
};

data_spec_get(#gri{aspect = download_url}) -> #{
    required => #{<<"file_ids">> => {list_of_binaries, guid}},
    optional => #{<<"follow_symlinks">> => {boolean, any}}
}.


%% @private
-spec authorize_get(middleware:req(), middleware:entity()) -> boolean().
authorize_get(#op_req{gri = #gri{id = FileGuid, aspect = As, scope = public}}, _) when
    As =:= instance;
    As =:= children;
    As =:= children_details;
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata;
    As =:= symlink_value;
    As =:= symlink_target
->
    file_id:is_share_guid(FileGuid);

authorize_get(#op_req{auth = Auth, gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= children;
    As =:= children_details;
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata;
    As =:= distribution;
    As =:= acl;
    As =:= shares;
    As =:= dataset_summary;
    As =:= hardlinks;
    As =:= symlink_value;
    As =:= symlink_target;
    As =:= archive_recall_details;
    As =:= archive_recall_progress
->
    middleware_utils:has_access_to_file_space(Auth, Guid);

authorize_get(#op_req{auth = Auth, gri = #gri{id = FirstGuid, aspect = {hardlinks, SecondGuid}}}, _) ->
    middleware_utils:has_access_to_file_space(Auth, FirstGuid) andalso
        middleware_utils:has_access_to_file_space(Auth, SecondGuid);

authorize_get(#op_req{auth = ?USER(UserId), gri = #gri{id = Guid, aspect = transfers}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_TRANSFERS);

authorize_get(#op_req{auth = ?USER(UserId), gri = #gri{id = Guid, aspect = qos_summary}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_QOS);

authorize_get(#op_req{auth = Auth, gri = #gri{aspect = download_url, scope = Scope}, data = Data}, _) ->
    Predicate = case Scope of
        private -> 
            fun(Guid) -> 
                not file_id:is_share_guid(Guid) 
                    andalso middleware_utils:has_access_to_file_space(Auth, Guid) 
            end;
        public -> 
            fun file_id:is_share_guid/1
    end,
    lists:all(Predicate, maps:get(<<"file_ids">>, Data)).


%% @private
-spec validate_get(middleware:req(), middleware:entity()) -> ok | no_return().
validate_get(#op_req{gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= children;
    As =:= children_details;
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata;
    As =:= distribution;
    As =:= acl;
    As =:= shares;
    As =:= transfers;
    As =:= qos_summary;
    As =:= dataset_summary;
    As =:= hardlinks;
    As =:= symlink_value;
    As =:= symlink_target;
    As =:= archive_recall_details;
    As =:= archive_recall_progress
->
    middleware_utils:assert_file_managed_locally(Guid);

validate_get(#op_req{gri = #gri{id = FirstGuid, aspect = {hardlinks, SecondGuid}}}, _) ->
    middleware_utils:assert_file_managed_locally(FirstGuid),
    middleware_utils:assert_file_managed_locally(SecondGuid);

validate_get(#op_req{gri = #gri{aspect = download_url}, data = Data}, _) ->
    FileIds = maps:get(<<"file_ids">>, Data),
    lists:foreach(fun(Guid) ->
        middleware_utils:assert_file_managed_locally(Guid)
    end, FileIds).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = instance}}, _) ->
    ?lfm_check(lfm:get_details(Auth#auth.session_id, ?FILE_REF(FileGuid)));

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = children}}, _) ->
    SessionId = Auth#auth.session_id,

    {ok, Children, #{is_last := IsLast}} = ?lfm_check(lfm:get_children(
        SessionId, ?FILE_REF(FileGuid), #{
            offset => maps:get(<<"offset">>, Data, ?DEFAULT_LIST_OFFSET),
            size => maps:get(<<"limit">>, Data, ?DEFAULT_LIST_ENTRIES),
            last_name => maps:get(<<"index">>, Data, undefined)
    })),
    {ok, value, {Children, IsLast}};

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = children_details}}, _) ->
    SessionId = Auth#auth.session_id,

    {ok, ChildrenDetails, #{is_last := IsLast}} = ?lfm_check(lfm:get_children_details(
        SessionId, ?FILE_REF(FileGuid), #{
            offset => maps:get(<<"offset">>, Data, ?DEFAULT_LIST_OFFSET),
            size => maps:get(<<"limit">>, Data, ?DEFAULT_LIST_ENTRIES),
            last_name => maps:get(<<"index">>, Data, undefined)
        }
    )),
    {ok, value, {ChildrenDetails, IsLast}};

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = attrs, scope = Sc}}, _) ->
    RequestedAttributes = case maps:get(<<"attribute">>, Data, undefined) of
        undefined ->
            case Sc of
                private -> ?PRIVATE_BASIC_ATTRIBUTES;
                public -> ?PUBLIC_BASIC_ATTRIBUTES
            end;
        Attr ->
            [Attr]
    end,
    {ok, FileAttrs} = ?lfm_check(lfm:stat(Auth#auth.session_id, ?FILE_REF(FileGuid), true)),

    {ok, value, maps:with(RequestedAttributes, file_attrs_to_json(FileAttrs))};

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = xattrs}}, _) ->
    SessionId = Auth#auth.session_id,
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),

    Inherited = maps:get(<<"inherited">>, Data, false),
    ShowInternal = maps:get(<<"show_internal">>, Data, false),

    case maps:get(<<"attribute">>, Data, undefined) of
        undefined ->
            {ok, Xattrs} = ?lfm_check(lfm:list_xattr(
                SessionId, FileRef, Inherited, ShowInternal
            )),
            {ok, value, lists:foldl(fun(XattrName, Acc) ->
                {ok, #xattr{value = Value}} = ?lfm_check(lfm:get_xattr(
                    SessionId, FileRef, XattrName, Inherited
                )),
                Acc#{XattrName => Value}
            end, #{}, Xattrs)};
        XattrName ->
            {ok, #xattr{value = Val}} = ?lfm_check(lfm:get_xattr(
                SessionId, FileRef, XattrName, Inherited
            )),
            {ok, value, #{XattrName => Val}}
    end;

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = json_metadata}}, _) ->
    SessionId = Auth#auth.session_id,
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),

    Inherited = maps:get(<<"inherited">>, Data, false),
    FilterType = maps:get(<<"filter_type">>, Data, undefined),
    Filter = maps:get(<<"filter">>, Data, undefined),

    FilterList = case {FilterType, Filter} of
        {undefined, _} ->
            [];
        {<<"keypath">>, undefined} ->
            throw(?ERROR_MISSING_REQUIRED_VALUE(<<"filter">>));
        {<<"keypath">>, _} ->
            binary:split(Filter, <<".">>, [global])
    end,

    {ok, Result} = ?lfm_check(lfm:get_metadata(SessionId, FileRef, json, FilterList, Inherited)),
    {ok, value, Result};

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = rdf_metadata}}, _) ->
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),

    {ok, Result} = ?lfm_check(lfm:get_metadata(Auth#auth.session_id, FileRef, rdf, [], false)),
    {ok, value, Result};

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = acl}}, _) ->
    ?lfm_check(lfm:get_acl(Auth#auth.session_id, ?FILE_REF(FileGuid)));

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = distribution}}, _) ->
    ?lfm_check(lfm:get_file_distribution(Auth#auth.session_id, ?FILE_REF(FileGuid)));

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = shares}}, _) ->
    {ok, FileAttrs} = ?lfm_check(lfm:stat(Auth#auth.session_id, ?FILE_REF(FileGuid))),
    {ok, FileAttrs#file_attr.shares};

get(#op_req{data = Data, gri = #gri{id = FileGuid, aspect = transfers}}, _) ->
    {ok, #{
        ongoing := Ongoing,
        ended := Ended
    }} = transferred_file:get_transfers(FileGuid),

    Transfers = #{
        <<"ongoingIds">> => Ongoing,
        <<"endedCount">> => length(Ended)
    },
    case maps:get(<<"include_ended_ids">>, Data, false) of
        true ->
            {ok, value, Transfers#{<<"endedIds">> => Ended}};
        false ->
            {ok, value, Transfers}
    end;

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = qos_summary}}, _) ->
    {QosEntriesWithStatus, _AssignedEntries} = mi_qos:get_effective_file_qos(
        Auth#auth.session_id, ?FILE_REF(FileGuid)
    ),
    {ok, #{
        <<"requirements">> => QosEntriesWithStatus,
        <<"status">> => qos_status:aggregate(maps:values(QosEntriesWithStatus))
    }};

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = dataset_summary}}, _) ->
    {ok, mi_datasets:get_file_eff_summary(Auth#auth.session_id, ?FILE_REF(FileGuid))};

get(#op_req{auth = Auth, gri = #gri{aspect = download_url}, data = Data}, _) ->
    SessionId = Auth#auth.session_id,
    FileGuids = maps:get(<<"file_ids">>, Data),
    FollowSymlinks = maps:get(<<"follow_symlinks">>, Data, true),
    case page_file_download:gen_file_download_url(SessionId, FileGuids, FollowSymlinks) of
        {ok, URL} ->
            {ok, value, URL};
        {error, _} = Error ->
            Error
    end;

get(#op_req{auth = ?USER(_UserId, SessId), data = Data, gri = #gri{id = FileGuid, aspect = hardlinks}}, _) ->
    {ok, Hardlinks} = Result = ?lfm_check(lfm:get_file_references(
        SessId, ?FILE_REF(FileGuid)
    )),
    case maps:get(<<"limit">>, Data, undefined) of
        undefined ->
            Result;
        Limit ->
            {ok, lists:sublist(Hardlinks, Limit)}
    end;

get(#op_req{gri = #gri{id = FirstGuid, aspect = {hardlinks, SecondGuid}}}, _) ->
    FirstReferencedUuid = fslogic_uuid:ensure_referenced_uuid(file_id:guid_to_uuid(FirstGuid)),
    SecondReferencedUuid = fslogic_uuid:ensure_referenced_uuid(file_id:guid_to_uuid(SecondGuid)),
    case SecondReferencedUuid of
        FirstReferencedUuid -> {ok, #{}};
        _ -> ?ERROR_NOT_FOUND
    end;

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = symlink_value}}, _) ->
    ?lfm_check(lfm:read_symlink(Auth#auth.session_id, ?FILE_REF(FileGuid)));

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = symlink_target, scope = Scope}}, _) ->
    SessionId = Auth#auth.session_id,

    {ok, TargetFileGuid} = ?lfm_check(lfm:resolve_symlink(SessionId, ?FILE_REF(FileGuid))),
    {ok, TargetFileDetails} = ?lfm_check(lfm:get_details(SessionId, ?FILE_REF(TargetFileGuid))),

    TargetFileGri = #gri{
        type = op_file, id = TargetFileGuid,
        aspect = instance, scope = Scope
    },
    {ok, TargetFileGri, TargetFileDetails};


get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = archive_recall_details}}, _) ->
    {ok, mi_archives:get_recall_details(Auth#auth.session_id, FileGuid)};


get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = archive_recall_progress}}, _) ->
    {ok, mi_archives:get_recall_progress(Auth#auth.session_id, FileGuid)}.


%%%===================================================================
%%% UPDATE SECTION
%%%===================================================================


%% @private
-spec resolve_update_operation_handler(gri:aspect(), middleware:scope()) ->
    module() | no_return().
resolve_update_operation_handler(instance, private) -> ?MODULE;              % gs only
resolve_update_operation_handler(acl, private) -> ?MODULE;
resolve_update_operation_handler(_, _) -> throw(?ERROR_NOT_SUPPORTED).


%% @private
-spec data_spec_update(gri:gri()) -> undefined | middleware_sanitizer:data_spec().
data_spec_update(#gri{aspect = instance}) ->
    ModeParam = <<"posixPermissions">>,

    #{required => #{
        id => {binary, guid},
        ModeParam => {binary, fun(Mode) ->
            try binary_to_integer(Mode, 8) of
                ValidMode when ValidMode >= 0 andalso ValidMode =< 8#777 ->
                    {true, ValidMode};
                _ ->
                    throw(?ERROR_BAD_VALUE_NOT_IN_RANGE(ModeParam, 0, 8#777))
            catch _:_ ->
                throw(?ERROR_BAD_VALUE_INTEGER(ModeParam))
            end
        end}
    }};

data_spec_update(#gri{aspect = acl}) -> #{
    required => #{
        id => {binary, guid},
        <<"list">> => {any, fun(JsonAcl) ->
            try
                {true, acl:from_json(JsonAcl, gui)}
            catch throw:{error, Errno} ->
                throw(?ERROR_POSIX(Errno))
            end
        end}
    }
}.


%% @private
-spec authorize_update(middleware:req(), middleware:entity()) -> boolean().
authorize_update(#op_req{auth = Auth, gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= acl
->
    middleware_utils:has_access_to_file_space(Auth, Guid).


%% @private
-spec validate_update(middleware:req(), middleware:entity()) -> ok | no_return().
validate_update(#op_req{gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= acl
->
    middleware_utils:assert_file_managed_locally(Guid).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{auth = Auth, data = Data, gri = #gri{id = Guid, aspect = instance}}) ->
    case maps:get(<<"posixPermissions">>, Data, undefined) of
        undefined ->
            ok;
        PosixPerms ->
            ?lfm_check(lfm:set_perms(Auth#auth.session_id, ?FILE_REF(Guid), PosixPerms))
    end;
update(#op_req{auth = Auth, data = Data, gri = #gri{id = Guid, aspect = acl}}) ->
    ?lfm_check(lfm:set_acl(
        Auth#auth.session_id,
        ?FILE_REF(Guid),
        maps:get(<<"list">>, Data)
    )).


%%%===================================================================
%%% DELETE SECTION
%%%===================================================================


%% @private
-spec resolve_delete_operation_handler(gri:aspect(), middleware:scope()) ->
    module() | no_return().
resolve_delete_operation_handler(instance, private) -> ?MODULE;              % gs only
resolve_delete_operation_handler(xattrs, private) -> ?MODULE;                % REST/gs
resolve_delete_operation_handler(json_metadata, private) -> ?MODULE;         % REST/gs
resolve_delete_operation_handler(rdf_metadata, private) -> ?MODULE;          % REST/gs
resolve_delete_operation_handler(_, _) -> throw(?ERROR_NOT_SUPPORTED).


%% @private
-spec data_spec_delete(gri:gri()) -> undefined | middleware_sanitizer:data_spec().
data_spec_delete(#gri{aspect = instance}) ->
    #{required => #{id => {binary, guid}}};

data_spec_delete(#gri{aspect = As}) when
    As =:= json_metadata;
    As =:= rdf_metadata
->
    #{
        required => #{id => {binary, guid}},
        optional => #{<<"resolve_symlink">> => {boolean, any}}
    };

data_spec_delete(#gri{aspect = xattrs}) -> #{
    required => #{
        id => {binary, guid},
        <<"keys">> => {list_of_binaries, any}
    },
    optional => #{
        <<"resolve_symlink">> => {boolean, any}
    }
}.


%% @private
-spec authorize_delete(middleware:req(), middleware:entity()) -> boolean().
authorize_delete(#op_req{auth = Auth, gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata
->
    middleware_utils:has_access_to_file_space(Auth, Guid).


%% @private
-spec validate_delete(middleware:req(), middleware:entity()) -> ok | no_return().
validate_delete(#op_req{gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata
->
    middleware_utils:assert_file_managed_locally(Guid).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{auth = ?USER(_UserId, SessionId), gri = #gri{id = FileGuid, aspect = instance}}) ->
    FileRef = ?FILE_REF(FileGuid),

    case ?lfm_check(lfm:stat(SessionId, FileRef)) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
            ?lfm_check(lfm:rm_recursive(SessionId, FileRef));
        {ok, _} ->
            ?lfm_check(lfm:unlink(SessionId, FileRef, false))
    end;

delete(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = xattrs}}) ->
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),

    lists:foreach(fun(XattrName) ->
        ?lfm_check(lfm:remove_xattr(Auth#auth.session_id, FileRef, XattrName))
    end, maps:get(<<"keys">>, Data));

delete(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = json_metadata}}) ->
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),
    ?lfm_check(lfm:remove_metadata(Auth#auth.session_id, FileRef, json));

delete(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = rdf_metadata}}) ->
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),
    ?lfm_check(lfm:remove_metadata(Auth#auth.session_id, FileRef, rdf)).


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
