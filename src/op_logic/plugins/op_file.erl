%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations (create, get, update, delete)
%%% corresponding to file aspects such as:
%%% - attributes,
%%% - extended attributes,
%%% - json metadata,
%%% - rdf metadata.
%%% @end
%%%-------------------------------------------------------------------
-module(op_file).
-author("Bartosz Walkowicz").

-behaviour(op_logic_behaviour).

-include("op_logic.hrl").
-include_lib("ctool/include/api_errors.hrl").

-export([op_logic_plugin/0]).
-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    exists/2,
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

-define(run(__FunctionCall), check_result(__FunctionCall)).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns the op logic plugin module that handles model logic.
%% @end
%%--------------------------------------------------------------------
op_logic_plugin() ->
    op_file.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), op_logic:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(create, instance, private) -> true;
operation_supported(create, attrs, private) -> true;
operation_supported(create, xattrs, private) -> true;
operation_supported(create, json_metadata, private) -> true;
operation_supported(create, rdf_metadata, private) -> true;

operation_supported(get, instance, private) -> true;
operation_supported(get, list, private) -> true;
operation_supported(get, attrs, private) -> true;
operation_supported(get, xattrs, private) -> true;
operation_supported(get, json_metadata, private) -> true;
operation_supported(get, rdf_metadata, private) -> true;

operation_supported(delete, instance, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(op_logic:req()) -> undefined | op_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = instance}}) -> #{
    required => #{
        <<"name">> => {binary, non_empty},
        <<"type">> => {binary, [<<"file">>, <<"dir">>]},
        <<"parent">> => {binary, fun(Parent) ->
            try gs_protocol:string_to_gri(Parent) of
                #gri{type = op_file, id = ParentGuid, aspect = instance} ->
                    {true, ParentGuid};
                _ ->
                    throw(?ERROR_BAD_VALUE_IDENTIFIER(<<"parent">>))
            catch _:_ ->
                false
            end
        end}

    }
};

data_spec(#op_req{operation = create, gri = #gri{aspect = attrs}}) -> #{
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

data_spec(#op_req{operation = create, gri = #gri{aspect = xattrs}}) -> #{
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

data_spec(#op_req{operation = create, gri = #gri{aspect = json_metadata}}) -> #{
    required => #{<<"application/json">> => {any, any}},
    optional => #{
        <<"filter_type">> => {binary, [<<"keypath">>]},
        <<"filter">> => {binary, any}
    }
};

data_spec(#op_req{operation = create, gri = #gri{aspect = rdf_metadata}}) -> #{
    required => #{<<"application/rdf+xml">> => {binary, any}}
};

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = list}}) -> #{
    optional => #{
        <<"limit">> => {integer, {between, 1, 1000}},
        <<"offset">> => {integer, {not_lower_than, 0}}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = attrs}}) -> #{
    optional => #{<<"attribute">> => {binary, ?ALL_BASIC_ATTRIBUTES}}
};

data_spec(#op_req{operation = get, gri = #gri{aspect = xattrs}}) -> #{
    optional => #{
        <<"attribute">> => {binary, any},
        <<"inherited">> => {boolean, any}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = json_metadata}}) -> #{
    optional => #{
        <<"filter_type">> => {binary, any},
        <<"filter">> => {binary, any},
        <<"inherited">> => {boolean, any}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = rdf_metadata}}) ->
    undefined;

data_spec(#op_req{operation = delete, gri = #gri{aspect = instance}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(op_logic:req()) ->
    {ok, op_logic:entity()} | op_logic:error().
fetch_entity(_) ->
    {ok, undefined}.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback exists/2.
%%
%% File existence is checked later by fslogic_worker as to not increase
%% overhead.
%% @end
%%--------------------------------------------------------------------
-spec exists(op_logic:req(), op_logic:entity()) -> boolean().
exists(_, _) ->
    true.

%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback authorize/2.
%%
%% Checks only if user is in space in which file exists. File permissions
%% are checked later by logical_file_manager (lfm).
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), op_logic:entity()) -> boolean().
authorize(#op_req{auth = ?NOBODY}, _) ->
    false;

authorize(#op_req{operation = create, gri = #gri{aspect = instance}} = Req, _) ->
    SpaceId = file_id:guid_to_space_id(maps:get(<<"parent">>, Req#op_req.data)),
    op_logic_utils:is_eff_space_member(Req#op_req.auth, SpaceId);

authorize(#op_req{operation = create, gri = #gri{id = Guid, aspect = As}} = Req, _) when
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata
->
    check_space_membership(Req#op_req.auth, Guid);

authorize(#op_req{operation = get, gri = #gri{id = Guid, aspect = As}} = Req, _) when
    As =:= instance;
    As =:= list;
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata
->
    check_space_membership(Req#op_req.auth, Guid);

authorize(#op_req{operation = delete, gri = #gri{id = Guid, aspect = instance}} = Req, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:is_eff_space_member(Req#op_req.auth, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req(), op_logic:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{aspect = instance}} = Req, _) ->
    SpaceId = file_id:guid_to_space_id(maps:get(<<"parent">>, Req#op_req.data)),
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = create, gri = #gri{id = Guid, aspect = As}} = Req, _) when
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata
->
    assert_space_supported_locally(Req#op_req.auth, Guid);

validate(#op_req{operation = get, gri = #gri{id = Guid, aspect = As}} = Req, _) when
    As =:= instance;
    As =:= list;
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata
->
    assert_space_supported_locally(Req#op_req.auth, Guid);

validate(#op_req{operation = delete, gri = #gri{id = Guid, aspect = instance}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#op_req{auth = Auth, data = Data, gri = #gri{aspect = instance} = GRI}) ->
    SessionId = Auth#auth.session_id,

    Name = maps:get(<<"name">>, Data),
    Type = maps:get(<<"type">>, Data),
    ParentGuid = maps:get(<<"parent">>, Data),

    {ok, ParentPath} = ?run(lfm:get_file_path(SessionId, ParentGuid)),
    Path = filename:join([ParentPath, Name]),
    {ok, Guid} = case Type of
        <<"file">> -> ?run(lfm:create(SessionId, Path));
        <<"dir">> -> ?run(lfm:mkdir(SessionId, Path))
    end,

    {ok, Attrs} = ?run(lfm:stat(SessionId, {guid, Guid})),
    {ok, resource, {GRI#gri{id = Guid}, Attrs}};

create(#op_req{auth = Auth, data = Data, gri = #gri{id = Guid, aspect = attrs}}) ->
    Mode = maps:get(<<"mode">>, Data),
    ?run(lfm:set_perms(Auth#auth.session_id, {guid, Guid}, Mode));

create(#op_req{auth = Auth, data = Data, gri = #gri{id = Guid, aspect = xattrs}}) ->
    [{Name, Value}] = maps:to_list(maps:get(<<"application/json">>, Data)),
    Xattr = #xattr{name = Name, value = Value},
    ?run(lfm:set_xattr(Auth#auth.session_id, {guid, Guid}, Xattr, false, false));

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
    ?run(lfm:set_metadata(Auth#auth.session_id, {guid, Guid}, json, JSON, FilterList));

create(#op_req{auth = Auth, data = Data, gri = #gri{id = Guid, aspect = rdf_metadata}}) ->
    Rdf = maps:get(<<"application/rdf+xml">>, Data),
    ?run(lfm:set_metadata(Auth#auth.session_id, {guid, Guid}, rdf, Rdf, [])).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = instance}}, _) ->
    ?run(lfm:stat(Auth#auth.session_id, {guid, FileGuid}));

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = list}}, _) ->
    SessionId = Auth#auth.session_id,
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_LIST_ENTRIES),
    Offset = maps:get(<<"offset">>, Data, ?DEFAULT_LIST_OFFSET),
    {ok, Path} = ?run(lfm:get_file_path(SessionId, FileGuid)),

    case lfm:stat(SessionId, {guid, FileGuid}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE, guid = Guid}} ->
            {ok, Children} = ?run(lfm:ls(SessionId, {guid, Guid}, Offset, Limit)),
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

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = attrs}}, _) ->
    SessionId = Auth#auth.session_id,
    Attributes = case maps:get(<<"attribute">>, Data, undefined) of
        undefined -> ?ALL_BASIC_ATTRIBUTES;
        Attr -> [Attr]
    end,
    {ok, Attrs} = ?run(lfm:stat(SessionId, {guid, FileGuid})),
    {ok, gather_attributes(#{}, Attributes, Attrs)};

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = xattrs}}, _) ->
    SessionId = Auth#auth.session_id,
    Inherited = maps:get(<<"inherited">>, Data, false),

    case maps:get(<<"attribute">>, Data, undefined) of
        undefined ->
            {ok, Xattrs} = ?run(lfm:list_xattr(
                SessionId, {guid, FileGuid}, Inherited, true
            )),
            {ok, lists:foldl(fun(XattrName, Acc) ->
                {ok, #xattr{value = Value}} = ?run(lfm:get_xattr(
                    SessionId,
                    {guid, FileGuid},
                    XattrName,
                    Inherited
                )),
                Acc#{XattrName => Value}
            end, #{}, Xattrs)};
        XattrName ->
            {ok, #xattr{value = Val}} = ?run(lfm:get_xattr(
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

    ?run(lfm:get_metadata(SessionId, {guid, FileGuid}, json, FilterList, Inherited));

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = rdf_metadata}}, _) ->
    ?run(lfm:get_metadata(Auth#auth.session_id, {guid, FileGuid}, rdf, [], false)).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = instance}}) ->
    ?run(lfm:rm_recursive(Auth#auth.session_id, {guid, FileGuid})).


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
-spec check_space_membership(aai:auth(), file_id:file_guid()) -> boolean().
check_space_membership(?USER(UserId) = Auth, Guid) ->
    case fslogic_uuid:user_root_dir_guid(UserId) of
        Guid ->
            true;
        _ ->
            SpaceId = file_id:guid_to_space_id(Guid),
            op_logic_utils:is_eff_space_member(Auth, SpaceId)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asserts that space containing specified file is supported by this provider.
%% Omit this check in case of user root dir which doesn't belong to any space
%% and can be reached from any provider.
%% @end
%%--------------------------------------------------------------------
-spec assert_space_supported_locally(aai:auth(), file_id:file_guid()) ->
    ok | no_return().
assert_space_supported_locally(?USER(UserId), Guid) ->
    case fslogic_uuid:user_root_dir_guid(UserId) of
        Guid ->
            ok;
        _ ->
            SpaceId = file_id:guid_to_space_id(Guid),
            op_logic_utils:assert_space_supported_locally(SpaceId)
    end.


-spec check_result(ok | {ok, term()} | {error, term()}) ->
    ok | {ok, term()} | no_return().
check_result(ok) -> ok;
check_result({ok, _} = Res) -> Res;
check_result({error, Errno}) -> throw(?ERROR_POSIX(Errno)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds attributes listed in list, to given map.
%% @end
%%--------------------------------------------------------------------
-spec gather_attributes(maps:map(), list(), #file_attr{}) -> maps:map().
gather_attributes(Map, [], _Attr) ->
    Map;
gather_attributes(Map, [<<"mode">> | Rest], Attr = #file_attr{mode = Mode}) ->
    maps:put(<<"mode">>, <<"0", (integer_to_binary(Mode, 8))/binary>>, gather_attributes(Map, Rest, Attr));
gather_attributes(Map, [<<"size">> | Rest], Attr = #file_attr{size = Size}) ->
    maps:put(<<"size">>, Size, gather_attributes(Map, Rest, Attr));
gather_attributes(Map, [<<"atime">> | Rest], Attr = #file_attr{atime = ATime}) ->
    maps:put(<<"atime">>, ATime, gather_attributes(Map, Rest, Attr));
gather_attributes(Map, [<<"ctime">> | Rest], Attr = #file_attr{ctime = CTime}) ->
    maps:put(<<"ctime">>, CTime, gather_attributes(Map, Rest, Attr));
gather_attributes(Map, [<<"mtime">> | Rest], Attr = #file_attr{mtime = MTime}) ->
    maps:put(<<"mtime">>, MTime, gather_attributes(Map, Rest, Attr));
gather_attributes(Map, [<<"storage_group_id">> | Rest], Attr = #file_attr{gid = Gid}) ->
    maps:put(<<"storage_group_id">>, Gid, gather_attributes(Map, Rest, Attr));
gather_attributes(Map, [<<"storage_user_id">> | Rest], Attr = #file_attr{uid = Gid}) ->
    maps:put(<<"storage_user_id">>, Gid, gather_attributes(Map, Rest, Attr));
gather_attributes(Map, [<<"name">> | Rest], Attr = #file_attr{name = Name}) ->
    maps:put(<<"name">>, Name, gather_attributes(Map, Rest, Attr));
gather_attributes(Map, [<<"owner_id">> | Rest], Attr = #file_attr{owner_id = OwnerId}) ->
    maps:put(<<"owner_id">>, OwnerId, gather_attributes(Map, Rest, Attr));
gather_attributes(Map, [<<"shares">> | Rest], Attr = #file_attr{shares = Shares}) ->
    maps:put(<<"shares">>, Shares, gather_attributes(Map, Rest, Attr));
gather_attributes(Map, [<<"type">> | Rest], Attr = #file_attr{type = ?REGULAR_FILE_TYPE}) ->
    maps:put(<<"type">>, <<"reg">>, gather_attributes(Map, Rest, Attr));
gather_attributes(Map, [<<"type">> | Rest], Attr = #file_attr{type = ?DIRECTORY_TYPE}) ->
    maps:put(<<"type">>, <<"dir">>, gather_attributes(Map, Rest, Attr));
gather_attributes(Map, [<<"type">> | Rest], Attr = #file_attr{type = ?SYMLINK_TYPE}) ->
    maps:put(<<"type">>, <<"lnk">>, gather_attributes(Map, Rest, Attr));
gather_attributes(Map, [<<"file_id">> | Rest], Attr = #file_attr{guid = Guid}) ->
    {ok, Id} = file_id:guid_to_objectid(Guid),
    maps:put(<<"file_id">>, Id, gather_attributes(Map, Rest, Attr)).
