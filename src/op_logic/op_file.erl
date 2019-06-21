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
-include_lib("ctool/include/api_errors.hrl").

-export([op_logic_plugin/0]).
-export([operation_supported/3]).
-export([create/1, get/2, update/1, delete/1]).
-export([authorize/2, data_signature/1]).

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
operation_supported(create, attributes, private) -> true;
operation_supported(create, metadata, private) -> true;

operation_supported(get, list, private) -> true;
operation_supported(get, attributes, private) -> true;
operation_supported(get, metadata, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% Creates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = attributes}}) ->
    SessionId = Cl#client.id,
    Extended = maps:get(<<"extended">>, Data, false),
    AttributeBody = maps:get(<<"application/json">>, Data),

    {Attr, Val} = parse_attribute_body(AttributeBody, Extended),

    case {Attr, Extended} of
        {<<"mode">>, false} ->
            ok = logical_file_manager:set_perms(SessionId, {guid, FileGuid}, Val);
        {_, true} when is_binary(Attr) ->
            ok = logical_file_manager:set_xattr(
                SessionId, {guid, FileGuid},
                #xattr{name = Attr, value = Val},
                false, false
            )
    end;

create(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = metadata}}) ->
    ok;

create(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a resource (aspect of entity) based on op logic request and
%% prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = list}}, _) ->
    SessionId = Cl#client.id,
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_LIST_ENTRIES),
    Offset = maps:get(<<"offset">>, Data, ?DEFAULT_LIST_OFFSET),

    {ok, Path} = logical_file_manager:get_file_path(SessionId, FileGuid),

    case logical_file_manager:stat(SessionId, {guid, FileGuid}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE, guid = Guid}} ->
            {ok, Children} = logical_file_manager:ls(SessionId, {guid, FileGuid}, Offset, Limit),
            {ok, lists:map(fun({ChildGuid, ChildPath}) ->
                    {ok, ObjectId} = file_id:guid_to_objectid(ChildGuid),
                    #{<<"id">> => ObjectId, <<"path">> => filename:join(Path, ChildPath)}
            end, Children)};
        {ok, #file_attr{guid = Guid}} ->
            {ok, ObjectId} = file_id:guid_to_objectid(Guid),
            {ok, [#{<<"id">> => ObjectId, <<"path">> => Path}]};
        Error ->
            Error
   end;

get(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = attributes}}, _) ->
    SessionId = Cl#client.id,
    Extended = maps:get(<<"extended">>, Data, false),
    Inherited = maps:get(<<"inherited">>, Data, false),
    Attribute = maps:get(<<"attribute">>, Data, undefined),

    case {Attribute, Extended} of
        {undefined, false} ->
            {ok, Attrs} = logical_file_manager:stat(SessionId, {guid, FileGuid}),
            {ok, add_attr(#{}, ?ALL_BASIC_ATTRIBUTES, Attrs)};
        {Attribute, false} ->
            case lists:member(Attribute, ?ALL_BASIC_ATTRIBUTES) of
                true ->
                    {ok, Attrs} = logical_file_manager:stat(SessionId, {guid, FileGuid}),
                    {ok, add_attr(#{}, [Attribute], Attrs)};
                false ->
                    ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attribute">>, ?ALL_BASIC_ATTRIBUTES)
            end;
        {undefined, true} ->
            {ok, Xattrs} = logical_file_manager:list_xattr(
                SessionId, {guid, FileGuid}, Inherited, true
            ),
            {ok, lists:foldl(fun(XattrName, Acc) ->
                {ok, #xattr{value = Value}} = logical_file_manager:get_xattr(
                    SessionId,
                    {guid, FileGuid},
                    XattrName,
                    Inherited
                ),
                Acc#{XattrName => Value}
            end, #{}, Xattrs)};
        {XattrName, true} ->
            {ok, #xattr{value = Value}} = logical_file_manager:get_xattr(
                SessionId, {guid, FileGuid}, XattrName, Inherited
            ),
            {ok, #{XattrName => Value}}
    end;

get(#op_req{client = Cl, gri = #gri{id = FileGuid, aspect = metadata}}, _) ->
    ok;

get(_, _) ->
    ?ERROR_NOT_SUPPORTED.


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


%%--------------------------------------------------------------------
%% @doc
%% Determines if requesting client is authorized to perform given operation,
%% based on op logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), entity_logic:entity()) -> boolean().
authorize(_, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% Returns data signature for given request.
%% Returns a map with 'required', 'optional' and 'at_least_one' keys.
%% Under each of them, there is a map:
%%      Key => {type_constraint, value_constraint}
%% Which means how value of given Key should be validated.
%% @end
%%--------------------------------------------------------------------
-spec data_signature(op_logic:req()) -> op_validator:data_signature().
data_signature(#op_req{operation = create, gri = #gri{aspect = attributes}}) -> #{
    required => #{<<"application/json">> => {json, any}},
    optional => #{<<"extended">> => {boolean, any}}
};

data_signature(#op_req{operation = create, gri = #gri{aspect = metadata}}) -> #{
    optional => #{
        <<"metadata_type">> => {binary, [<<"JSON">>, <<"RDF">>]},
        <<"filter_type">> => {binary, any},
        <<"filter">> => {binary, any}
    }
};

data_signature(#op_req{operation = get, gri = #gri{aspect = list}}) -> #{
    optional => #{
        <<"limit">> => {integer, {between, 1, 1000}},
        <<"offset">> => {integer, {not_lower_than, 0}}
    }
};

data_signature(#op_req{operation = get, gri = #gri{aspect = attributes}}) -> #{
    optional => #{
        <<"attribute">> => {binary, any},
        <<"extended">> => {boolean, any},
        <<"inherited">> => {boolean, any}
    }
};

data_signature(#op_req{operation = get, gri = #gri{aspect = metadata}}) -> #{
    optional => #{
        <<"metadata_type">> => {binary, [<<"JSON">>, <<"RDF">>]},
        <<"filter_type">> => {binary, any},
        <<"filter">> => {binary, any},
        <<"inherited">> => {boolean, any}
    }
};

data_signature(_) -> #{}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves request's attribute from body and parses it. In case of
%% basic attributes only "mode" can be modified.
%% @end
%%--------------------------------------------------------------------
-spec parse_attribute_body(cowboy_req:req(), maps:map()) ->
    {binary(), term()} | no_return().
parse_attribute_body(Json, Extended) ->
    case {maps:to_list(Json), Extended} of
        {[{<<"mode">>, Value}], false} ->
            try binary_to_integer(Value, 8) of
                Mode ->
                    {<<"mode">>, Mode}
            catch
                _:_ ->
                    throw(?ERROR_BAD_VALUE_INTEGER(<<"mode">>))
            end;
        {[{_Attr, _Value}], false} ->
            throw(?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attribute">>, [<<"mode">>]));
        {[{Attr, _Value}], true} when not is_binary(Attr) ->
            throw(?ERROR_BAD_VALUE_BINARY(<<"attribute name">>));
        {[{Attr, Value}], true} ->
            {Attr, Value};
        {_, _} ->
            throw(?ERROR_BAD_DATA(<<"attribute">>))
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds attributes listed in list, to given map.
%% @end
%%--------------------------------------------------------------------
-spec add_attr(maps:map(), list(), #file_attr{}) -> maps:map().
add_attr(Map, [], _Attr) ->
    Map;
add_attr(Map, [<<"mode">> | Rest], Attr = #file_attr{mode = Mode}) ->
    maps:put(<<"mode">>, <<"0", (integer_to_binary(Mode, 8))/binary>>, add_attr(Map, Rest, Attr));
add_attr(Map, [<<"size">> | Rest], Attr = #file_attr{size = Size}) ->
    maps:put(<<"size">>, Size, add_attr(Map, Rest, Attr));
add_attr(Map, [<<"atime">> | Rest], Attr = #file_attr{atime = ATime}) ->
    maps:put(<<"atime">>, ATime, add_attr(Map, Rest, Attr));
add_attr(Map, [<<"ctime">> | Rest], Attr = #file_attr{ctime = CTime}) ->
    maps:put(<<"ctime">>, CTime, add_attr(Map, Rest, Attr));
add_attr(Map, [<<"mtime">> | Rest], Attr = #file_attr{mtime = MTime}) ->
    maps:put(<<"mtime">>, MTime, add_attr(Map, Rest, Attr));
add_attr(Map, [<<"storage_group_id">> | Rest], Attr = #file_attr{gid = Gid}) ->
    maps:put(<<"storage_group_id">>, Gid, add_attr(Map, Rest, Attr));
add_attr(Map, [<<"storage_user_id">> | Rest], Attr = #file_attr{uid = Gid}) ->
    maps:put(<<"storage_user_id">>, Gid, add_attr(Map, Rest, Attr));
add_attr(Map, [<<"name">> | Rest], Attr = #file_attr{name = Name}) ->
    maps:put(<<"name">>, Name, add_attr(Map, Rest, Attr));
add_attr(Map, [<<"owner_id">> | Rest], Attr = #file_attr{owner_id = OwnerId}) ->
    maps:put(<<"owner_id">>, OwnerId, add_attr(Map, Rest, Attr));
add_attr(Map, [<<"shares">> | Rest], Attr = #file_attr{shares = Shares}) ->
    maps:put(<<"shares">>, Shares, add_attr(Map, Rest, Attr));
add_attr(Map, [<<"type">> | Rest], Attr = #file_attr{type = ?REGULAR_FILE_TYPE}) ->
    maps:put(<<"type">>, <<"reg">>, add_attr(Map, Rest, Attr));
add_attr(Map, [<<"type">> | Rest], Attr = #file_attr{type = ?DIRECTORY_TYPE}) ->
    maps:put(<<"type">>, <<"dir">>, add_attr(Map, Rest, Attr));
add_attr(Map, [<<"type">> | Rest], Attr = #file_attr{type = ?SYMLINK_TYPE}) ->
    maps:put(<<"type">>, <<"lnk">>, add_attr(Map, Rest, Attr));
add_attr(Map, [<<"file_id">> | Rest], Attr = #file_attr{guid = Guid}) ->
    {ok, Id} = file_id:guid_to_objectid(Guid),
    maps:put(<<"file_id">>, Id, add_attr(Map, Rest, Attr)).
