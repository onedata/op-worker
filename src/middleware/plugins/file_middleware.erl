%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
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
-module(file_middleware).
-author("Bartosz Walkowicz").

-behaviour(middleware_plugin).

-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).

-define(DEFAULT_LIST_OFFSET, 0).
-define(DEFAULT_LIST_ENTRIES, 1000).

-define(ALL_BASIC_ATTRIBUTES, [
    <<"mode">>, <<"size">>, <<"atime">>, <<"ctime">>,
    <<"mtime">>, <<"storage_group_id">>, <<"storage_user_id">>, <<"name">>,
    <<"owner_id">>, <<"shares">>, <<"type">>, <<"file_id">>
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(middleware:operation(), gri:aspect(),
    middleware:scope()) -> boolean().
operation_supported(create, Aspect, Scope) ->
    create_operation_supported(Aspect, Scope);
operation_supported(get, Aspect, Scope) ->
    get_operation_supported(Aspect, Scope);
operation_supported(update, Aspect, Scope) ->
    update_operation_supported(Aspect, Scope);
operation_supported(delete, Aspect, Scope) ->
    delete_operation_supported(Aspect, Scope).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = GRI}) ->
    data_spec_create(GRI);
data_spec(#op_req{operation = get, gri = GRI}) ->
    data_spec_get(GRI);
data_spec(#op_req{operation = update, gri = GRI}) ->
    data_spec_update(GRI);
data_spec(#op_req{operation = delete, gri = GRI}) ->
    data_spec_delete(GRI).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(_) ->
    {ok, {undefined, 1}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback authorize/2.
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
%% {@link middleware_plugin} callback validate/2.
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
-spec create_operation_supported(gri:aspect(), middleware:scope()) ->
    boolean().
create_operation_supported(instance, private) -> true;
create_operation_supported(attrs, private) -> true;
create_operation_supported(xattrs, private) -> true;
create_operation_supported(json_metadata, private) -> true;
create_operation_supported(rdf_metadata, private) -> true;
create_operation_supported(_, _) -> false.


%% @private
-spec data_spec_create(gri:gri()) -> undefined | middleware_sanitizer:data_spec().
data_spec_create(#gri{aspect = instance}) -> #{
    required => #{
        <<"name">> => {binary, non_empty},
        <<"type">> => {binary, [<<"file">>, <<"dir">>]},
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
    optional => #{<<"createAttempts">> => {integer, {between, 1, 200}}}
};

data_spec_create(#gri{aspect = attrs}) -> #{
    required => #{<<"mode">> => {binary,
        fun(Mode) ->
            try
                {true, binary_to_integer(Mode, 8)}
            catch
                _:_ ->
                    throw(?ERROR_BAD_VALUE_INTEGER(<<"mode">>))
            end
        end
    }}
};

data_spec_create(#gri{aspect = xattrs}) -> #{
    required => #{<<"application/json">> => {json,
        % Accept only one xattr to set
        fun(JSON) ->
            case maps:to_list(JSON) of
                [{Key, _Val}] when not is_binary(Key) ->
                    throw(?ERROR_BAD_VALUE_BINARY(<<"extended attribute name">>));
                [{_, _}] ->
                    true;
                _ ->
                    false
            end
        end
    }}
};

data_spec_create(#gri{aspect = json_metadata}) -> #{
    required => #{<<"application/json">> => {any, any}},
    optional => #{
        <<"filter_type">> => {binary, [<<"keypath">>]},
        <<"filter">> => {binary, any}
    }
};

data_spec_create(#gri{aspect = rdf_metadata}) -> #{
    required => #{<<"application/rdf+xml">> => {binary, any}}
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
    has_access_to_file(Auth, Guid).


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
    assert_file_managed_locally(Guid).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = Auth, data = Data, gri = #gri{aspect = instance} = GRI}) ->
    SessionId = Auth#auth.session_id,

    {ok, Guid} = create_file(
        SessionId,
        maps:get(<<"parent">>, Data),
        maps:get(<<"name">>, Data),
        binary_to_atom(maps:get(<<"type">>, Data), utf8),
        0,
        maps:get(<<"createAttempts">>, Data, 1)
    ),

    {ok, Attrs} = ?check(lfm:stat(SessionId, {guid, Guid})),
    {ok, resource, {GRI#gri{id = Guid}, Attrs}};

create(#op_req{auth = Auth, data = Data, gri = #gri{id = Guid, aspect = attrs}}) ->
    Mode = maps:get(<<"mode">>, Data),
    ?check(lfm:set_perms(Auth#auth.session_id, {guid, Guid}, Mode));

create(#op_req{auth = Auth, data = Data, gri = #gri{id = Guid, aspect = xattrs}}) ->
    [{Name, Value}] = maps:to_list(maps:get(<<"application/json">>, Data)),
    ?check(lfm:set_xattr(
        Auth#auth.session_id, {guid, Guid},
        #xattr{name = Name, value = Value},
        false, false
    ));

create(#op_req{auth = Auth, data = Data, gri = #gri{id = Guid, aspect = json_metadata}}) ->
    JSON = maps:get(<<"application/json">>, Data),
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
    ?check(lfm:set_metadata(
        Auth#auth.session_id, {guid, Guid},
        json, JSON, FilterList
    ));

create(#op_req{auth = Auth, data = Data, gri = #gri{id = Guid, aspect = rdf_metadata}}) ->
    Rdf = maps:get(<<"application/rdf+xml">>, Data),
    ?check(lfm:set_metadata(
        Auth#auth.session_id, {guid, Guid},
        rdf, Rdf, []
    )).


%%%===================================================================
%%% GET SECTION
%%%===================================================================


-spec get_operation_supported(gri:gri(), middleware:scope()) ->
    boolean().
get_operation_supported(instance, private) -> true;
get_operation_supported(instance, public) -> true;
get_operation_supported(object_id, private) -> true;
get_operation_supported(list, private) -> true;
get_operation_supported(children, private) -> true;
get_operation_supported(children, public) -> true;
get_operation_supported(attrs, private) -> true;
get_operation_supported(xattrs, private) -> true;
get_operation_supported(json_metadata, private) -> true;
get_operation_supported(rdf_metadata, private) -> true;
get_operation_supported(acl, private) -> true;
get_operation_supported(shares, private) -> true;
get_operation_supported(transfers, private) -> true;
get_operation_supported(download_url, private) -> true;
get_operation_supported(download_url, public) -> true;
get_operation_supported(_, _) -> false.


%% @private
-spec data_spec_get(gri:gri()) -> undefined | middleware_sanitizer:data_spec().
data_spec_get(#gri{aspect = instance}) ->
    undefined;

data_spec_get(#gri{aspect = object_id}) ->
    undefined;

data_spec_get(#gri{aspect = list}) -> #{
    optional => #{
        <<"limit">> => {integer, {between, 1, 1000}},
        <<"offset">> => {integer, {not_lower_than, 0}}
    }
};

data_spec_get(#gri{aspect = children}) -> #{
    required => #{
        <<"limit">> => {integer, {not_lower_than, 1}}
    },
    optional => #{
        <<"index">> => {any, fun
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

data_spec_get(#gri{aspect = attrs}) -> #{
    optional => #{<<"attribute">> => {binary, ?ALL_BASIC_ATTRIBUTES}}
};

data_spec_get(#gri{aspect = xattrs}) -> #{
    optional => #{
        <<"attribute">> => {binary, any},
        <<"inherited">> => {boolean, any}
    }
};

data_spec_get(#gri{aspect = json_metadata}) -> #{
    optional => #{
        <<"filter_type">> => {binary, any},
        <<"filter">> => {binary, any},
        <<"inherited">> => {boolean, any}
    }
};

data_spec_get(#gri{aspect = rdf_metadata}) ->
    undefined;

data_spec_get(#gri{aspect = acl}) ->
    undefined;

data_spec_get(#gri{aspect = shares}) ->
    undefined;

data_spec_get(#gri{aspect = transfers}) -> #{
    optional => #{<<"include_ended_ids">> => {boolean, any}}
};

data_spec_get(#gri{aspect = download_url}) ->
    undefined.


%% @private
-spec authorize_get(middleware:req(), middleware:entity()) -> boolean().
authorize_get(#op_req{gri = #gri{aspect = As, scope = public}}, _) when
    As =:= instance;
    As =:= children;
    As =:= download_url
->
    true;

authorize_get(#op_req{auth = Auth, gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= list;
    As =:= children;
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata;
    As =:= acl;
    As =:= shares;
    As =:= download_url
->
    has_access_to_file(Auth, Guid);

authorize_get(#op_req{gri = #gri{aspect = object_id}}, _) ->
    % File path must have been resolved to guid by rest_handler already (to
    % get to this point), so authorization is surely granted.
    true;

authorize_get(#op_req{auth = ?USER(UserId), gri = #gri{id = Guid, aspect = transfers}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_TRANSFERS).


%% @private
-spec validate_get(middleware:req(), middleware:entity()) -> ok | no_return().
validate_get(#op_req{gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= list;
    As =:= children;
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata;
    As =:= acl;
    As =:= shares;
    As =:= transfers;
    As =:= download_url
->
    assert_file_managed_locally(Guid);

validate_get(#op_req{gri = #gri{aspect = object_id}}, _) ->
    % File path must have been resolved to guid by rest_handler already (to
    % get to this point), so file must be managed locally.
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = instance}}, _) ->
    ?check(lfm:stat(Auth#auth.session_id, {guid, FileGuid}));

get(#op_req{gri = #gri{id = FileGuid, aspect = object_id}}, _) ->
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    {ok, #{<<"fileId">> => ObjectId}};

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = list}}, _) ->
    SessionId = Auth#auth.session_id,
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_LIST_ENTRIES),
    Offset = maps:get(<<"offset">>, Data, ?DEFAULT_LIST_OFFSET),
    {ok, Path} = ?check(lfm:get_file_path(SessionId, FileGuid)),

    case lfm:stat(SessionId, {guid, FileGuid}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE, guid = Guid}} ->
            {ok, Children} = ?check(lfm:ls(SessionId, {guid, Guid}, Offset, Limit)),
            {ok, lists:map(fun({ChildGuid, ChildPath}) ->
                {ok, ObjectId} = file_id:guid_to_objectid(ChildGuid),
                #{<<"id">> => ObjectId, <<"path">> => filename:join(Path, ChildPath)}
            end, Children)};
        {ok, #file_attr{guid = Guid}} ->
            {ok, ObjectId} = file_id:guid_to_objectid(Guid),
            {ok, [#{<<"id">> => ObjectId, <<"path">> => Path}]};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end;

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = children}}, _) ->
    SessionId = Auth#auth.session_id,
    Limit = maps:get(<<"limit">>, Data),
    StartId = maps:get(<<"index">>, Data, undefined),
    Offset = maps:get(<<"offset">>, Data, 0),

    case lfm:ls(SessionId, {guid, FileGuid}, Offset, Limit, undefined, StartId) of
        {ok, Children, _, _} ->
            {ok, value, Children};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end;

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = attrs}}, _) ->
    RequestedAttributes = case maps:get(<<"attribute">>, Data, undefined) of
        undefined -> ?ALL_BASIC_ATTRIBUTES;
        Attr -> [Attr]
    end,
    {ok, FileAttrs} = ?check(lfm:stat(Auth#auth.session_id, {guid, FileGuid})),

    {ok, lists:foldl(fun(RequestedAttr, Acc) ->
        Acc#{RequestedAttr => get_attr(RequestedAttr, FileAttrs)}
    end, #{}, RequestedAttributes)};

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = xattrs}}, _) ->
    SessionId = Auth#auth.session_id,
    Inherited = maps:get(<<"inherited">>, Data, false),

    case maps:get(<<"attribute">>, Data, undefined) of
        undefined ->
            {ok, Xattrs} = ?check(lfm:list_xattr(
                SessionId, {guid, FileGuid}, Inherited, true
            )),
            {ok, lists:foldl(fun(XattrName, Acc) ->
                {ok, #xattr{value = Value}} = ?check(lfm:get_xattr(
                    SessionId,
                    {guid, FileGuid},
                    XattrName,
                    Inherited
                )),
                Acc#{XattrName => Value}
            end, #{}, Xattrs)};
        XattrName ->
            {ok, #xattr{value = Val}} = ?check(lfm:get_xattr(
                SessionId, {guid, FileGuid}, XattrName, Inherited
            )),
            {ok, #{XattrName => Val}}
    end;

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = json_metadata}}, _) ->
    SessionId = Auth#auth.session_id,

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

    ?check(lfm:get_metadata(
        SessionId, {guid, FileGuid},
        json, FilterList, Inherited
    ));

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = rdf_metadata}}, _) ->
    ?check(lfm:get_metadata(
        Auth#auth.session_id, {guid, FileGuid},
        rdf, [], false
    ));

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = acl}}, _) ->
    ?check(lfm:get_acl(Auth#auth.session_id, {guid, FileGuid}));

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = shares}}, _) ->
    case lfm:stat(Auth#auth.session_id, {guid, FileGuid}) of
        {ok, #file_attr{shares = Shares}} ->
            {ok, Shares};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end;

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

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = download_url}}, _) ->
    SessionId = Auth#auth.session_id,
    case page_file_download:get_file_download_url(SessionId, FileGuid) of
        {ok, URL} ->
            {ok, value, URL};
        ?ERROR_FORBIDDEN ->
            ?ERROR_FORBIDDEN;
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


%%%===================================================================
%%% UPDATE SECTION
%%%===================================================================


%% @private
-spec update_operation_supported(gri:aspect(), middleware:scope()) ->
    boolean().
update_operation_supported(instance, private) -> true;
update_operation_supported(acl, private) -> true;
update_operation_supported(_, _) -> false.


%% @private
-spec data_spec_update(gri:gri()) -> undefined | middleware_sanitizer:data_spec().
data_spec_update(#gri{aspect = instance}) -> #{
    optional => #{
        <<"posixPermissions">> => {binary,
            fun(Mode) ->
                try
                    {true, binary_to_integer(Mode, 8)}
                catch _:_ ->
                    throw(?ERROR_BAD_VALUE_INTEGER(<<"posixPermissions">>))
                end
            end
        }
    }
};

data_spec_update(#gri{aspect = acl}) -> #{
    required => #{
        <<"list">> => {any,
            fun(JsonAcl) ->
                try
                    {true, acl:from_json(JsonAcl, gui)}
                catch throw:{error, Errno} ->
                    throw(?ERROR_POSIX(Errno))
                end
            end
        }
    }
}.


%% @private
-spec authorize_update(middleware:req(), middleware:entity()) -> boolean().
authorize_update(#op_req{auth = Auth, gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= acl
->
    has_access_to_file(Auth, Guid).


%% @private
-spec validate_update(middleware:req(), middleware:entity()) -> ok | no_return().
validate_update(#op_req{gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= acl
->
    assert_file_managed_locally(Guid).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{auth = Auth, data = Data, gri = #gri{id = Guid, aspect = instance}}) ->
    case maps:get(<<"posixPermissions">>, Data, undefined) of
        undefined ->
            ok;
        PosixPerms ->
            ?check(lfm:set_perms(Auth#auth.session_id, {guid, Guid}, PosixPerms))
    end;
update(#op_req{auth = Auth, data = Data, gri = #gri{id = Guid, aspect = acl}}) ->
    ?check(lfm:set_acl(
        Auth#auth.session_id,
        {guid, Guid},
        maps:get(<<"list">>, Data)
    )).


%%%===================================================================
%%% DELETE SECTION
%%%===================================================================


%% @private
-spec delete_operation_supported(gri:aspect(), middleware:scope()) ->
    boolean().
delete_operation_supported(instance, private) -> true;
delete_operation_supported(_, _) -> false.


%% @private
-spec data_spec_delete(gri:gri()) -> undefined | middleware_sanitizer:data_spec().
data_spec_delete(#gri{aspect = instance}) ->
    undefined.


%% @private
-spec authorize_delete(middleware:req(), middleware:entity()) -> boolean().
authorize_delete(#op_req{auth = Auth, gri = #gri{id = Guid, aspect = instance}}, _) ->
    has_access_to_file(Auth, Guid).


%% @private
-spec validate_delete(middleware:req(), middleware:entity()) -> ok | no_return().
validate_delete(#op_req{gri = #gri{id = Guid, aspect = instance}}, _) ->
    assert_file_managed_locally(Guid).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = instance}}) ->
    ?check(lfm:rm_recursive(Auth#auth.session_id, {guid, FileGuid})).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks user membership in space containing specified file. Returns true
%% in case of user root dir since it doesn't belong to any space.
%% @end
%%--------------------------------------------------------------------
-spec has_access_to_file(aai:auth(), file_id:file_guid()) -> boolean().
has_access_to_file(?GUEST, _Guid) ->
    false;
has_access_to_file(?USER(UserId) = Auth, Guid) ->
    case fslogic_uuid:user_root_dir_guid(UserId) of
        Guid ->
            true;
        _ ->
            SpaceId = file_id:guid_to_space_id(Guid),
            middleware_utils:is_eff_space_member(Auth, SpaceId)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asserts that space containing specified file is supported by this provider.
%% Omit this check in case of user root dir which doesn't belong to any space
%% and can be reached from any provider.
%% @end
%%--------------------------------------------------------------------
-spec assert_file_managed_locally(file_id:file_guid()) ->
    ok | no_return().
assert_file_managed_locally(FileGuid) ->
    {FileUuid, SpaceId} = file_id:unpack_guid(FileGuid),
    case fslogic_uuid:is_root_dir_uuid(FileUuid) of
        true ->
            ok;
        false ->
            middleware_utils:assert_space_supported_locally(SpaceId)
    end.


%% @private
-spec create_file(session:id(), file_id:file_guid(), file_meta:name(), file | dir) ->
    {ok, file_id:file_guid()} | {error, term()}.
create_file(SessionId, ParentGuid, Name, file) ->
    lfm:create(SessionId, ParentGuid, Name, undefined);
create_file(SessionId, ParentGuid, Name, dir) ->
    lfm:mkdir(SessionId, ParentGuid, Name, undefined).


%% @private
-spec create_file(
    SessionId :: session:id(),
    ParentGuid :: file_id:file_guid(),
    Name :: file_meta:name(),
    Type :: file | dir,
    Counter :: non_neg_integer(),
    Attempts :: non_neg_integer()
) ->
    {ok, file_id:file_guid()} | no_return().
create_file(_, _, _, _, Counter, Attempts) when Counter >= Attempts ->
    throw(?ERROR_POSIX(?EEXIST));
create_file(SessId, ParentGuid, OriginalName, Type, Counter, Attempts) ->
    Name = maybe_add_file_suffix(OriginalName, Counter),
    case create_file(SessId, ParentGuid, Name, Type) of
        {ok, Guid} ->
            {ok, Guid};
        {error, ?EEXIST} ->
            create_file(
                SessId, ParentGuid, OriginalName, Type,
                Counter + 1, Attempts
            );
        {error, Errno} ->
            throw(?ERROR_POSIX(Errno))
    end.


%% @private
-spec maybe_add_file_suffix(file_meta:name(), Counter :: non_neg_integer()) ->
    file_meta:name().
maybe_add_file_suffix(OriginalName, 0) ->
    OriginalName;
maybe_add_file_suffix(OriginalName, Counter) ->
    RootName = filename:rootname(OriginalName),
    Ext = filename:extension(OriginalName),
    str_utils:format_bin("~s(~B)~s", [RootName, Counter, Ext]).


%% @private
-spec get_attr(binary(), #file_attr{}) -> term().
get_attr(<<"mode">>, #file_attr{mode = Mode}) ->
    <<"0", (integer_to_binary(Mode, 8))/binary>>;
get_attr(<<"name">>, #file_attr{name = Name}) -> Name;
get_attr(<<"size">>, #file_attr{size = Size}) -> Size;
get_attr(<<"atime">>, #file_attr{atime = ATime}) -> ATime;
get_attr(<<"ctime">>, #file_attr{ctime = CTime}) -> CTime;
get_attr(<<"mtime">>, #file_attr{mtime = MTime}) -> MTime;
get_attr(<<"owner_id">>, #file_attr{owner_id = OwnerId}) -> OwnerId;
get_attr(<<"type">>, #file_attr{type = ?REGULAR_FILE_TYPE}) -> <<"reg">>;
get_attr(<<"type">>, #file_attr{type = ?DIRECTORY_TYPE}) -> <<"dir">>;
get_attr(<<"type">>, #file_attr{type = ?SYMLINK_TYPE}) -> <<"lnk">>;
get_attr(<<"shares">>, #file_attr{shares = Shares}) -> Shares;
get_attr(<<"storage_user_id">>, #file_attr{uid = Uid}) -> Uid;
get_attr(<<"storage_group_id">>, #file_attr{gid = Gid}) -> Gid;
get_attr(<<"file_id">>, #file_attr{guid = Guid}) ->
    {ok, Id} = file_id:guid_to_objectid(Guid),
    Id.
