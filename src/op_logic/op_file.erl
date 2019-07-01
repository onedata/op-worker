%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations corresponding to op_file model.
%%% @end
%%%-------------------------------------------------------------------
-module(op_file).
-author("Bartosz Walkowicz").

-behaviour(op_logic_behaviour).

-include("op_logic.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
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

-define(call_lfm(__FunctionCall), extract_lfm_res(logical_file_manager:__FunctionCall)).

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
%% Determines if given operation is supported based on operation, aspect and
%% scope (entity type is known based on the plugin itself).
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), op_logic:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(create, attrs, private) -> true;
operation_supported(create, xattrs, private) -> true;
operation_supported(create, json_metadata, private) -> true;
operation_supported(create, rdf_metadata, private) -> true;

operation_supported(get, list, private) -> true;
operation_supported(get, attrs, private) -> true;
operation_supported(get, xattrs, private) -> true;
operation_supported(get, json_metadata, private) -> true;
operation_supported(get, rdf_metadata, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% Returns data spec for given request.
%% Returns a map with 'required', 'optional' and 'at_least_one' keys.
%% Under each of them, there is a map:
%%      Key => {type_constraint, value_constraint}
%% Which means how value of given Key should be validated.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(op_logic:req()) -> undefined | op_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = attrs}}) -> #{
    required => #{<<"application/json">> => {json,
        % Currently only file mode can be changed
        fun(JSON) ->
            case maps:to_list(JSON) of
                [{<<"mode">>, Value}] ->
                    try binary_to_integer(Value, 8) of
                        Val ->
                            {true, #{<<"mode">> => Val}}
                    catch
                        _:_ ->
                            throw(?ERROR_BAD_VALUE_INTEGER(<<"mode">>))
                    end;
                [{_Attr, _Value}] ->
                    throw(?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attribute">>, [<<"mode">>]));
                _ ->
                    false
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
    required => #{<<"application/json">> => {json, any}},
    optional => #{
        <<"filter_type">> => {binary, [<<"keypath">>]},
        <<"filter">> => {binary, any}
    }
};

data_spec(#op_req{operation = create, gri = #gri{aspect = rdf_metadata}}) -> #{
    required => #{<<"application/rdf+xml">> => {binary, any}}
};

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
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves an entity from datastore based on its EntityId.
%% Should return ?ERROR_NOT_FOUND if the entity does not exist.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(op_logic:entity_id()) ->
    {ok, op_logic:entity()} | entity_logic:error().
fetch_entity(_) ->
    {ok, undefined}.


%%--------------------------------------------------------------------
%% @doc
%% Determines if given resource (aspect of entity) exists, based on
%% op logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec exists(op_logic:req(), entity_logic:entity()) -> boolean().
exists(_, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% Determines if requesting client is authorized to perform given operation,
%% based on op logic request and prefetched entity.
%%
%% Checks only if user is in space in which file exists. File permissions
%% are checked later by logical_file_manager.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), entity_logic:entity()) -> boolean().
authorize(#op_req{client = ?NOBODY}, _) ->
    false;

authorize(#op_req{operation = create, gri = #gri{id = Guid, aspect = As}} = Req, _) when
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadta
->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId);

authorize(#op_req{operation = get, gri = #gri{id = Guid, aspect = list}} = Req, _) ->
    Client = Req#op_req.client,
    case fslogic_uuid:user_root_dir_guid(Client#client.id) of
        Guid ->
            true;
        _ ->
            SpaceId = file_id:guid_to_space_id(Guid),
            op_logic_utils:is_eff_space_member(Client, SpaceId)
    end;

authorize(#op_req{operation = get, gri = #gri{id = Guid, aspect = As}} = Req, _) when
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadta
->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Determines if given request can be further processed
%% (e.g. checks whether space is supported locally).
%% Should throw custom error if not (e.g. ?ERROR_SPACE_NOT_SUPPORTED).
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req(), entity_logic:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadta
->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:ensure_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = Guid, aspect = list}} = Req, _) ->
    Client = Req#op_req.client,
    case fslogic_uuid:user_root_dir_guid(Client#client.id) of
        Guid ->
            ok;
        _ ->
            SpaceId = file_id:guid_to_space_id(Guid),
            op_logic_utils:ensure_space_supported_locally(SpaceId)
    end;

validate(#op_req{operation = get, gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= attrs;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadta
->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:ensure_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Creates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = attrs}}) ->
    #{<<"mode">> := Mode} = maps:get(<<"application/json">>, Data),
    ?call_lfm(set_perms(Cl#client.session_id, {guid, FileGuid}, Mode));

create(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = xattrs}}) ->
    [{Name, Value}] = maps:to_list(maps:get(<<"application/json">>, Data)),
    Xattr = #xattr{name = Name, value = Value},
    ?call_lfm(set_xattr(Cl#client.session_id, {guid, FileGuid}, Xattr, false, false));

create(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = json_metadata}}) ->
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

    ?call_lfm(set_metadata(Cl#client.session_id, {guid, FileGuid}, json, JSON, FilterList));

create(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = rdf_metadata}}) ->
    Rdf = maps:get(<<"application/rdf+xml">>, Data),
    ?call_lfm(set_metadata(Cl#client.session_id, {guid, FileGuid}, rdf, Rdf, [])).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a resource (aspect of entity) based on op logic request and
%% prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = list}}, _) ->
    SessionId = Cl#client.session_id,
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_LIST_ENTRIES),
    Offset = maps:get(<<"offset">>, Data, ?DEFAULT_LIST_OFFSET),
    {ok, Path} = ?call_lfm(get_file_path(SessionId, FileGuid)),

    case logical_file_manager:stat(SessionId, {guid, FileGuid}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE, guid = Guid}} ->
            {ok, Children} = ?call_lfm(ls(SessionId, {guid, Guid}, Offset, Limit)),
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

get(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = attrs}}, _) ->
    SessionId = Cl#client.session_id,
    Attributes = case maps:get(<<"attribute">>, Data, undefined) of
        undefined -> ?ALL_BASIC_ATTRIBUTES;
        Attr -> [Attr]
    end,

    case logical_file_manager:stat(SessionId, {guid, FileGuid}) of
        {ok, Attrs} ->
            {ok, gather_attributes(#{}, Attributes, Attrs)};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end;

get(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = xattrs}}, _) ->
    SessionId = Cl#client.session_id,
    Inherited = maps:get(<<"inherited">>, Data, false),

    case maps:get(<<"attribute">>, Data, undefined) of
        undefined ->
            {ok, Xattrs} = ?call_lfm(list_xattr(
                SessionId, {guid, FileGuid}, Inherited, true
            )),
            {ok, lists:foldl(fun(XattrName, Acc) ->
                {ok, #xattr{value = Value}} = ?call_lfm(get_xattr(
                    SessionId,
                    {guid, FileGuid},
                    XattrName,
                    Inherited
                )),
                Acc#{XattrName => Value}
            end, #{}, Xattrs)};
        XattrName ->
            ?call_lfm(get_xattr(SessionId, {guid, FileGuid}, XattrName, Inherited))
    end;

get(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = json_metadata}}, _) ->
    SessionId = Cl#client.session_id,

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

    ?call_lfm(get_metadata(SessionId, {guid, FileGuid}, json, FilterList, Inherited));

get(#op_req{client = Cl, gri = #gri{id = FileGuid, aspect = rdf_metadata}}, _) ->
    ?call_lfm(get_metadata(Cl#client.id, {guid, FileGuid}, rdf, [], false)).


%%--------------------------------------------------------------------
%% @doc
%% Updates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% Deletes a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec extract_lfm_res({ok, term()} | {error, term()}) ->
    {ok, term()} | no_return().
extract_lfm_res({ok, _} = Res) -> Res;
extract_lfm_res({error, Errno}) -> throw(?ERROR_POSIX(Errno)).


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
