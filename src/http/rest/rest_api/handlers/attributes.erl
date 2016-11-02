%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for file attribute read and modification.
%%% @end
%%%--------------------------------------------------------------------
-module(attributes).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2, content_types_accepted/2]).

%% resource functions
-export([get_file_attributes/2, set_file_attribute/2]).

-define(ALL_BASIC_ATTRIBUTES, [<<"mode">>, <<"size">>, <<"atime">>, <<"ctime">>,
    <<"mtime">>, <<"storage_group_id">>, <<"storage_user_id">>, <<"name">>,
    <<"owner_id">>, <<"shares">>, <<"type">>, <<"file_id">>]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
rest_init(Req, State) ->
    {ok, Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), maps:map()) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), maps:map() | {error, term()}) -> {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"GET">>, <<"PUT">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | halt, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) -> {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_file_attributes}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, set_file_attribute}
    ], Req, State}.


%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/attributes/{path}'
%% @doc This method returns selected file attributes.
%%
%% HTTP method: GET
%%
%% @param path File path (e.g. &#39;/My Private Space/testfiles/file1.txt&#39;)
%% @param attribute Type of attribute to query for.
%%--------------------------------------------------------------------
-spec get_file_attributes(req(), maps:map()) -> {term(), req(), maps:map()}.
get_file_attributes(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    {StateWithExtended, ReqWithExtended} = validator:parse_extended(ReqWithPath, StateWithPath),
    {StateWithInherited, ReqWithInherited} = validator:parse_inherited(ReqWithExtended, StateWithExtended),
    {StateWithAttribute, ReqWithAttribute} = validator:parse_attribute(ReqWithInherited, StateWithInherited),

    #{auth := Auth, path := Path, attribute := Attribute, extended := Extended, inherited := Inherited} = StateWithAttribute,

    case {Attribute, Extended} of
        {undefined, false} ->
            {ok, Attrs} = onedata_file_api:stat(Auth, {path, Path}),
            ResponseMap = add_attr(#{}, ?ALL_BASIC_ATTRIBUTES, Attrs),
            Response = json_utils:encode_map(ResponseMap),
            {Response, ReqWithAttribute, StateWithAttribute};
        {Attribute, false} ->
            {ok, Attrs} = onedata_file_api:stat(Auth, {path, Path}),
            ResponseMap = add_attr(#{}, [Attribute], Attrs),
            Response = json_utils:encode_map(ResponseMap),
            {Response, ReqWithAttribute, StateWithAttribute};
        {undefined, true} ->
            {ok, Xattrs} = onedata_file_api:list_xattr(Auth, {path, Path}, Inherited, true),
            RawResponse = lists:map(fun(XattrName) ->
                {ok, #xattr{value = Value}} = onedata_file_api:get_xattr(Auth, {path, Path}, XattrName, Inherited),
                {XattrName, Value}
            end, Xattrs),
            Response = json_utils:encode_map(maps:from_list(RawResponse)),
            {Response, ReqWithAttribute, StateWithAttribute};
        {XattrName, true} ->
            {ok, #xattr{value = Value}} = onedata_file_api:get_xattr(Auth, {path, Path}, XattrName, Inherited),
            Response = json_utils:encode_map(#{XattrName => Value}),
            {Response, ReqWithAttribute, StateWithAttribute}
    end.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/attributes/{path}'
%% @doc This method allows to set a value of a specific file attribute (e.g. mode).
%%
%% HTTP method: PUT
%%
%% @param path File path (e.g. &#39;/My Private Space/testfiles/file1.txt&#39;)
%% @param attribute Attribute name and value.
%%--------------------------------------------------------------------
-spec set_file_attribute(req(), maps:map()) -> {term(), req(), maps:map()}.
set_file_attribute(Req, State) ->
    {State2, Req2} = validator:parse_path(Req, State),
    {State3, Req3} = validator:parse_extended(Req2, State2),
    {State4, Req4} = validator:parse_attribute_body(Req3, State3),

    #{attribute_body := {Attribute, Value}, path := Path, auth := Auth, extended := Extended} = State4,

    case {Attribute, Extended} of
        {<<"mode">>, false} ->
            ok = onedata_file_api:set_perms(Auth, {path, Path}, Value);
        {_, true} when is_binary(Attribute) ->
            ok = onedata_file_api:set_xattr(Auth, {path, Path}, #xattr{name = Attribute, value = Value})
    end,
    {true, Req4, State4}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
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
add_attr(Map, [<<"file_id">> | Rest], Attr = #file_attr{uuid = Uuid}) ->
    {ok, Id} = cdmi_id:uuid_to_objectid(Uuid),
    maps:put(<<"file_id">>, Id, add_attr(Map, Rest, Attr));
add_attr(_Map, _List, _Attr) ->
    throw(?ERROR_INVALID_ATTRIBUTE).